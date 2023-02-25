// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use ballista_core::config::{BallistaConfig, BALLISTA_JOB_NAME};
use ballista_core::serde::protobuf::execute_query_params::{OptionalSessionId, Query};
use std::convert::TryInto;

use ballista_core::serde::protobuf::executor_registration::OptionalHost;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpc;
use ballista_core::serde::protobuf::{
    executor_status, CancelJobParams, CancelJobResult, CleanJobDataParams,
    CleanJobDataResult, ExecuteQueryParams, ExecuteQueryResult, ExecutorHeartbeat,
    ExecutorStatus, ExecutorStoppedParams, ExecutorStoppedResult, GetFileMetadataParams,
    GetFileMetadataResult, GetJobStatusParams, GetJobStatusResult, HeartBeatParams,
    HeartBeatResult, PollWorkParams, PollWorkResult, RegisterExecutorParams,
    RegisterExecutorResult, UpdateTaskStatusParams, UpdateTaskStatusResult,
};
use ballista_core::serde::scheduler::ExecutorMetadata;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_substrait::serializer::deserialize_bytes;

use futures::TryStreamExt;
use log::{debug, error, info, trace, warn};
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};

use std::ops::Deref;
use std::sync::Arc;

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};

use crate::scheduler_server::{timestamp_secs, SchedulerServer};
use crate::state::executor_manager::ExecutorReservation;

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerGrpc
    for SchedulerServer<T, U>
{
    async fn poll_work(
        &self,
        request: Request<PollWorkParams>,
    ) -> Result<Response<PollWorkResult>, Status> {
        if self.state.config.is_push_staged_scheduling() {
            error!("Poll work interface is not supported for push-based task scheduling");
            return Err(tonic::Status::failed_precondition(
                "Bad request because poll work is not supported for push-based task scheduling",
            ));
        }
        let remote_addr = request.remote_addr();
        if let PollWorkParams {
            metadata: Some(metadata),
            num_free_slots,
            task_status,
        } = request.into_inner()
        {
            trace!("Received poll_work request for {:?}", metadata);
            // We might receive buggy poll work requests from dead executors.
            if self
                .state
                .executor_manager
                .is_dead_executor(&metadata.id.clone())
            {
                let error_msg = format!(
                    "Receive buggy poll work request from dead Executor {}",
                    metadata.id.clone()
                );
                warn!("{}", error_msg);
                return Err(Status::internal(error_msg));
            }
            let metadata = ExecutorMetadata {
                id: metadata.id,
                host: metadata
                    .optional_host
                    .map(|h| match h {
                        OptionalHost::Host(host) => host,
                    })
                    .unwrap_or_else(|| remote_addr.unwrap().ip().to_string()),
                port: metadata.port as u16,
                grpc_port: metadata.grpc_port as u16,
                specification: metadata.specification.unwrap().into(),
            };
            let executor_heartbeat = ExecutorHeartbeat {
                executor_id: metadata.id.clone(),
                timestamp: timestamp_secs(),
                metrics: vec![],
                status: Some(ExecutorStatus {
                    status: Some(executor_status::Status::Active("".to_string())),
                }),
            };

            self.state
                .executor_manager
                .save_executor_metadata(metadata.clone())
                .await
                .map_err(|e| {
                    let msg = format!("Could not save executor metadata: {e}");
                    error!("{}", msg);
                    Status::internal(msg)
                })?;

            self.state
                .executor_manager
                .save_executor_heartbeat(executor_heartbeat)
                .await
                .map_err(|e| {
                    let msg = format!("Could not save executor heartbeat: {e}");
                    error!("{}", msg);
                    Status::internal(msg)
                })?;

            self.update_task_status(&metadata.id, task_status)
                .await
                .map_err(|e| {
                    let msg = format!(
                        "Fail to update tasks status from executor {:?} due to {:?}",
                        &metadata.id, e
                    );
                    error!("{}", msg);
                    Status::internal(msg)
                })?;

            // Find `num_free_slots` next tasks when available
            let mut next_tasks = vec![];
            let reservations = vec![
                ExecutorReservation::new_free(metadata.id.clone());
                num_free_slots as usize
            ];
            if let Ok((mut assignments, _, _)) = self
                .state
                .task_manager
                .fill_reservations(&reservations)
                .await
            {
                while let Some((_, task)) = assignments.pop() {
                    match self.state.task_manager.prepare_task_definition(task) {
                        Ok(task_definition) => next_tasks.push(task_definition),
                        Err(e) => {
                            error!("Error preparing task definition: {:?}", e);
                        }
                    }
                }
            }

            Ok(Response::new(PollWorkResult { tasks: next_tasks }))
        } else {
            warn!("Received invalid executor poll_work request");
            Err(Status::invalid_argument("Missing metadata in request"))
        }
    }

    async fn register_executor(
        &self,
        request: Request<RegisterExecutorParams>,
    ) -> Result<Response<RegisterExecutorResult>, Status> {
        let remote_addr = request.remote_addr();
        if let RegisterExecutorParams {
            metadata: Some(metadata),
        } = request.into_inner()
        {
            info!("Received register executor request for {:?}", metadata);
            let metadata = ExecutorMetadata {
                id: metadata.id,
                host: metadata
                    .optional_host
                    .map(|h| match h {
                        OptionalHost::Host(host) => host,
                    })
                    .unwrap_or_else(|| remote_addr.unwrap().ip().to_string()),
                port: metadata.port as u16,
                grpc_port: metadata.grpc_port as u16,
                specification: metadata.specification.unwrap().into(),
            };

            self.do_register_executor(metadata).await.map_err(|e| {
                let msg = format!("Fail to do executor registration due to: {e}");
                error!("{}", msg);
                Status::internal(msg)
            })?;

            Ok(Response::new(RegisterExecutorResult { success: true }))
        } else {
            warn!("Received invalid register executor request");
            Err(Status::invalid_argument("Missing metadata in request"))
        }
    }

    async fn heart_beat_from_executor(
        &self,
        request: Request<HeartBeatParams>,
    ) -> Result<Response<HeartBeatResult>, Status> {
        let remote_addr = request.remote_addr();
        let HeartBeatParams {
            executor_id,
            metrics,
            status,
            metadata,
        } = request.into_inner();
        debug!("Received heart beat request for {:?}", executor_id);

        // If not registered, do registration first before saving heart beat
        if let Err(e) = self
            .state
            .executor_manager
            .get_executor_metadata(&executor_id)
            .await
        {
            warn!("Fail to get executor metadata: {}", e);
            if let Some(metadata) = metadata {
                let metadata = ExecutorMetadata {
                    id: metadata.id,
                    host: metadata
                        .optional_host
                        .map(|h| match h {
                            OptionalHost::Host(host) => host,
                        })
                        .unwrap_or_else(|| remote_addr.unwrap().ip().to_string()),
                    port: metadata.port as u16,
                    grpc_port: metadata.grpc_port as u16,
                    specification: metadata.specification.unwrap().into(),
                };

                self.do_register_executor(metadata).await.map_err(|e| {
                    let msg = format!("Fail to do executor registration due to: {e}");
                    error!("{}", msg);
                    Status::internal(msg)
                })?;
            } else {
                return Err(Status::invalid_argument(format!(
                    "The registration spec for executor {executor_id} is not included"
                )));
            }
        }

        let executor_heartbeat = ExecutorHeartbeat {
            executor_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            metrics,
            status,
        };

        self.state
            .executor_manager
            .save_executor_heartbeat(executor_heartbeat)
            .await
            .map_err(|e| {
                let msg = format!("Could not save executor heartbeat: {e}");
                error!("{}", msg);
                Status::internal(msg)
            })?;
        Ok(Response::new(HeartBeatResult { reregister: false }))
    }

    async fn update_task_status(
        &self,
        request: Request<UpdateTaskStatusParams>,
    ) -> Result<Response<UpdateTaskStatusResult>, Status> {
        let UpdateTaskStatusParams {
            executor_id,
            task_status,
        } = request.into_inner();

        debug!(
            "Received task status update request for executor {:?}",
            executor_id
        );

        self.update_task_status(&executor_id, task_status)
            .await
            .map_err(|e| {
                let msg = format!(
                    "Fail to update tasks status from executor {:?} due to {:?}",
                    &executor_id, e
                );
                error!("{}", msg);
                Status::internal(msg)
            })?;

        Ok(Response::new(UpdateTaskStatusResult { success: true }))
    }

    async fn get_file_metadata(
        &self,
        request: Request<GetFileMetadataParams>,
    ) -> Result<Response<GetFileMetadataResult>, Status> {
        // Here, we use the default config, since we don't know the session id
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        // TODO support multiple object stores
        let obj_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        // TODO shouldn't this take a ListingOption object as input?

        let GetFileMetadataParams { path, file_type } = request.into_inner();
        let file_format: Arc<dyn FileFormat> = match file_type.as_str() {
            "parquet" => Ok(Arc::new(ParquetFormat::default())),
            // TODO implement for CSV
            _ => Err(tonic::Status::unimplemented(
                "get_file_metadata unsupported file type",
            )),
        }?;

        let path = Path::from(path.as_str());
        let file_metas: Vec<_> = obj_store
            .list(Some(&path))
            .await
            .map_err(|e| {
                let msg = format!("Error listing files: {e}");
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?
            .try_collect()
            .await
            .map_err(|e| {
                let msg = format!("Error listing files: {e}");
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;

        let schema = file_format
            .infer_schema(&state, &obj_store, &file_metas)
            .await
            .map_err(|e| {
                let msg = format!("Error inferring schema: {e}");
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;

        Ok(Response::new(GetFileMetadataResult {
            schema: Some(schema.as_ref().try_into().map_err(|e| {
                let msg = format!("Error inferring schema: {e}");
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?),
        }))
    }

    async fn execute_query(
        &self,
        request: Request<ExecuteQueryParams>,
    ) -> Result<Response<ExecuteQueryResult>, Status> {
        let query_params = request.into_inner();
        if let ExecuteQueryParams {
            query: Some(query),
            settings,
            optional_session_id,
        } = query_params
        {
            // parse config
            let mut config_builder = BallistaConfig::builder();
            for kv_pair in &settings {
                config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
            }
            let config = config_builder.build().map_err(|e| {
                let msg = format!("Could not parse configs: {e}");
                error!("{}", msg);
                Status::internal(msg)
            })?;

            let (session_id, session_ctx) = match optional_session_id {
                Some(OptionalSessionId::SessionId(session_id)) => {
                    let ctx = self
                        .state
                        .session_manager
                        .update_session(&session_id, &config)
                        .await
                        .map_err(|e| {
                            Status::internal(format!(
                                "Failed to load SessionContext for session ID {session_id}: {e:?}"
                            ))
                        })?;
                    (session_id, ctx)
                }
                _ => {
                    let ctx = self
                        .state
                        .session_manager
                        .create_session(&config)
                        .await
                        .map_err(|e| {
                            Status::internal(format!(
                                "Failed to create SessionContext: {e:?}"
                            ))
                        })?;

                    (ctx.session_id(), ctx)
                }
            };

            let plan = match query {
                Query::SubstraitPlan(bytes) => {
                    let plan = deserialize_bytes(bytes).await.map_err(|e| {
                        let msg = format!("Could not parse substrait plan: {e}");
                        error!("{}", msg);
                        Status::internal(msg)
                    })?;

                    let mut ctx = session_ctx.as_ref().clone();
                    from_substrait_plan(&mut ctx, &plan).await.map_err(|e| {
                        let msg = format!("Could not parse substrait plan: {e}");
                        error!("{}", msg);
                        Status::internal(msg)
                    })?
                }
                Query::LogicalPlan(message) => T::try_decode(message.as_slice())
                    .and_then(|m| {
                        m.try_into_logical_plan(
                            session_ctx.deref(),
                            self.state.codec.logical_extension_codec(),
                        )
                    })
                    .map_err(|e| {
                        let msg = format!("Could not parse logical plan protobuf: {e}");
                        error!("{}", msg);
                        Status::internal(msg)
                    })?,
                Query::Sql(sql) => session_ctx
                    .sql(&sql)
                    .await
                    .and_then(|df| df.into_optimized_plan())
                    .map_err(|e| {
                        let msg = format!("Error parsing SQL: {e}");
                        error!("{}", msg);
                        Status::internal(msg)
                    })?,
            };

            debug!("Received plan for execution: {:?}", plan);

            let job_id = self.state.task_manager.generate_job_id();
            let job_name = config
                .settings()
                .get(BALLISTA_JOB_NAME)
                .cloned()
                .unwrap_or_default();

            self.submit_job(&job_id, &job_name, session_ctx, &plan)
                .await
                .map_err(|e| {
                    let msg =
                        format!("Failed to send JobQueued event for {job_id}: {e:?}");
                    error!("{}", msg);

                    Status::internal(msg)
                })?;

            Ok(Response::new(ExecuteQueryResult { job_id, session_id }))
        } else if let ExecuteQueryParams {
            query: None,
            settings,
            optional_session_id: None,
        } = query_params
        {
            // parse config for new session
            let mut config_builder = BallistaConfig::builder();
            for kv_pair in &settings {
                config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
            }
            let config = config_builder.build().map_err(|e| {
                let msg = format!("Could not parse configs: {e}");
                error!("{}", msg);
                Status::internal(msg)
            })?;
            let session = self
                .state
                .session_manager
                .create_session(&config)
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "Failed to create new SessionContext: {e:?}"
                    ))
                })?;

            Ok(Response::new(ExecuteQueryResult {
                job_id: "NA".to_owned(),
                session_id: session.session_id(),
            }))
        } else {
            Err(Status::internal("Error parsing request"))
        }
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusParams>,
    ) -> Result<Response<GetJobStatusResult>, Status> {
        let job_id = request.into_inner().job_id;
        trace!("Received get_job_status request for job {}", job_id);
        match self.state.task_manager.get_job_status(&job_id).await {
            Ok(status) => Ok(Response::new(GetJobStatusResult { status })),
            Err(e) => {
                let msg = format!("Error getting status for job {job_id}: {e:?}");
                error!("{}", msg);
                Err(Status::internal(msg))
            }
        }
    }

    async fn executor_stopped(
        &self,
        request: Request<ExecutorStoppedParams>,
    ) -> Result<Response<ExecutorStoppedResult>, Status> {
        let ExecutorStoppedParams {
            executor_id,
            reason,
        } = request.into_inner();
        info!(
            "Received executor stopped request from Executor {} with reason '{}'",
            executor_id, reason
        );

        let executor_manager = self.state.executor_manager.clone();
        let event_sender = self.query_stage_event_loop.get_sender().map_err(|e| {
            let msg = format!("Get query stage event loop error due to {e:?}");
            error!("{}", msg);
            Status::internal(msg)
        })?;

        Self::remove_executor(
            executor_manager,
            event_sender,
            &executor_id,
            Some(reason),
            self.executor_termination_grace_period,
        );

        Ok(Response::new(ExecutorStoppedResult {}))
    }

    async fn cancel_job(
        &self,
        request: Request<CancelJobParams>,
    ) -> Result<Response<CancelJobResult>, Status> {
        let job_id = request.into_inner().job_id;
        info!("Received cancellation request for job {}", job_id);

        self.query_stage_event_loop
            .get_sender()
            .map_err(|e| {
                let msg = format!("Get query stage event loop error due to {e:?}");
                error!("{}", msg);
                Status::internal(msg)
            })?
            .post_event(QueryStageSchedulerEvent::JobCancel(job_id))
            .await
            .map_err(|e| {
                let msg = format!("Post to query stage event loop error due to {e:?}");
                error!("{}", msg);
                Status::internal(msg)
            })?;
        Ok(Response::new(CancelJobResult { cancelled: true }))
    }

    async fn clean_job_data(
        &self,
        request: Request<CleanJobDataParams>,
    ) -> Result<Response<CleanJobDataResult>, Status> {
        let job_id = request.into_inner().job_id;
        info!("Received clean data request for job {}", job_id);

        self.query_stage_event_loop
            .get_sender()
            .map_err(|e| {
                let msg = format!("Get query stage event loop error due to {e:?}");
                error!("{}", msg);
                Status::internal(msg)
            })?
            .post_event(QueryStageSchedulerEvent::JobDataClean(job_id))
            .await
            .map_err(|e| {
                let msg = format!("Post to query stage event loop error due to {e:?}");
                error!("{}", msg);
                Status::internal(msg)
            })?;
        Ok(Response::new(CleanJobDataResult {}))
    }
}

#[cfg(all(test, feature = "sled"))]
mod test {

    use std::time::Duration;

    use datafusion_proto::protobuf::LogicalPlanNode;
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use tonic::Request;

    use crate::config::SchedulerConfig;
    use crate::metrics::default_metrics_collector;
    use ballista_core::error::BallistaError;
    use ballista_core::serde::protobuf::{
        executor_registration::OptionalHost, executor_status, ExecutorRegistration,
        ExecutorStatus, ExecutorStoppedParams, HeartBeatParams, PollWorkParams,
        RegisterExecutorParams,
    };
    use ballista_core::serde::scheduler::ExecutorSpecification;
    use ballista_core::serde::BallistaCodec;

    use crate::state::executor_manager::DEFAULT_EXECUTOR_TIMEOUT_SECONDS;
    use crate::state::SchedulerState;
    use crate::test_utils::await_condition;
    use crate::test_utils::test_cluster_context;

    use super::{SchedulerGrpc, SchedulerServer};

    #[tokio::test]
    async fn test_poll_work() -> Result<(), BallistaError> {
        let cluster = test_cluster_context();

        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                "localhost:50050".to_owned(),
                cluster.clone(),
                BallistaCodec::default(),
                SchedulerConfig::default(),
                default_metrics_collector().unwrap(),
            );
        scheduler.init().await?;
        let exec_meta = ExecutorRegistration {
            id: "abc".to_owned(),
            optional_host: Some(OptionalHost::Host("http://localhost:8080".to_owned())),
            port: 0,
            grpc_port: 0,
            specification: Some(ExecutorSpecification { task_slots: 2 }.into()),
        };
        let request: Request<PollWorkParams> = Request::new(PollWorkParams {
            metadata: Some(exec_meta.clone()),
            num_free_slots: 0,
            task_status: vec![],
        });
        let response = scheduler
            .poll_work(request)
            .await
            .expect("Received error response")
            .into_inner();
        // no response task since we told the scheduler we didn't want to accept one
        assert!(response.tasks.is_empty());
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new_with_default_scheduler_name(
                cluster.clone(),
                BallistaCodec::default(),
            );
        state.init().await?;

        // executor should be registered
        let stored_executor = state
            .executor_manager
            .get_executor_metadata("abc")
            .await
            .expect("getting executor");

        assert_eq!(stored_executor.grpc_port, 0);
        assert_eq!(stored_executor.port, 0);
        assert_eq!(stored_executor.specification.task_slots, 2);
        assert_eq!(stored_executor.host, "http://localhost:8080".to_owned());

        let request: Request<PollWorkParams> = Request::new(PollWorkParams {
            metadata: Some(exec_meta.clone()),
            num_free_slots: 1,
            task_status: vec![],
        });
        let response = scheduler
            .poll_work(request)
            .await
            .expect("Received error response")
            .into_inner();

        // still no response task since there are no tasks in the scheduler
        assert!(response.tasks.is_empty());
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new_with_default_scheduler_name(
                cluster.clone(),
                BallistaCodec::default(),
            );
        state.init().await?;

        // executor should be registered
        let stored_executor = state
            .executor_manager
            .get_executor_metadata("abc")
            .await
            .expect("getting executor");

        assert_eq!(stored_executor.grpc_port, 0);
        assert_eq!(stored_executor.port, 0);
        assert_eq!(stored_executor.specification.task_slots, 2);
        assert_eq!(stored_executor.host, "http://localhost:8080".to_owned());

        Ok(())
    }

    #[tokio::test]
    async fn test_stop_executor() -> Result<(), BallistaError> {
        let cluster = test_cluster_context();

        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                "localhost:50050".to_owned(),
                cluster.clone(),
                BallistaCodec::default(),
                SchedulerConfig::default().with_remove_executor_wait_secs(0),
                default_metrics_collector().unwrap(),
            );
        scheduler.init().await?;

        let exec_meta = ExecutorRegistration {
            id: "abc".to_owned(),
            optional_host: Some(OptionalHost::Host("http://localhost:8080".to_owned())),
            port: 0,
            grpc_port: 0,
            specification: Some(ExecutorSpecification { task_slots: 2 }.into()),
        };

        let request: Request<RegisterExecutorParams> =
            Request::new(RegisterExecutorParams {
                metadata: Some(exec_meta.clone()),
            });
        let response = scheduler
            .register_executor(request)
            .await
            .expect("Received error response")
            .into_inner();

        // registration should success
        assert!(response.success);

        let state = scheduler.state.clone();
        // executor should be registered
        let stored_executor = state
            .executor_manager
            .get_executor_metadata("abc")
            .await
            .expect("getting executor");

        assert_eq!(stored_executor.grpc_port, 0);
        assert_eq!(stored_executor.port, 0);
        assert_eq!(stored_executor.specification.task_slots, 2);
        assert_eq!(stored_executor.host, "http://localhost:8080".to_owned());

        let request: Request<ExecutorStoppedParams> =
            Request::new(ExecutorStoppedParams {
                executor_id: "abc".to_owned(),
                reason: "test_stop".to_owned(),
            });

        let _response = scheduler
            .executor_stopped(request)
            .await
            .expect("Received error response")
            .into_inner();

        // executor should be registered
        let _stopped_executor = state
            .executor_manager
            .get_executor_metadata("abc")
            .await
            .expect("getting executor");

        let is_stopped = await_condition(Duration::from_millis(10), 5, || {
            futures::future::ready(Ok(state.executor_manager.is_dead_executor("abc")))
        })
        .await?;

        // executor should be marked to dead
        assert!(is_stopped, "Executor not marked dead after 50ms");

        let active_executors = state
            .executor_manager
            .get_alive_executors_within_one_minute();
        assert!(active_executors.is_empty());

        let expired_executors = state
            .executor_manager
            .get_expired_executors(scheduler.executor_termination_grace_period);
        assert!(expired_executors.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_register_executor_in_heartbeat_service() -> Result<(), BallistaError> {
        let cluster = test_cluster_context();

        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                "localhost:50050".to_owned(),
                cluster,
                BallistaCodec::default(),
                SchedulerConfig::default(),
                default_metrics_collector().unwrap(),
            );
        scheduler.init().await?;

        let exec_meta = ExecutorRegistration {
            id: "abc".to_owned(),
            optional_host: Some(OptionalHost::Host("http://localhost:8080".to_owned())),
            port: 0,
            grpc_port: 0,
            specification: Some(ExecutorSpecification { task_slots: 2 }.into()),
        };

        let request: Request<HeartBeatParams> = Request::new(HeartBeatParams {
            executor_id: exec_meta.id.clone(),
            metrics: vec![],
            status: Some(ExecutorStatus {
                status: Some(executor_status::Status::Active("".to_string())),
            }),
            metadata: Some(exec_meta.clone()),
        });
        scheduler
            .heart_beat_from_executor(request)
            .await
            .expect("Received error response");

        let state = scheduler.state.clone();
        // executor should be registered
        let stored_executor = state
            .executor_manager
            .get_executor_metadata("abc")
            .await
            .expect("getting executor");

        assert_eq!(stored_executor.grpc_port, 0);
        assert_eq!(stored_executor.port, 0);
        assert_eq!(stored_executor.specification.task_slots, 2);
        assert_eq!(stored_executor.host, "http://localhost:8080".to_owned());

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_expired_executor() -> Result<(), BallistaError> {
        let cluster = test_cluster_context();

        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                "localhost:50050".to_owned(),
                cluster.clone(),
                BallistaCodec::default(),
                SchedulerConfig::default(),
                default_metrics_collector().unwrap(),
            );
        scheduler.init().await?;

        let exec_meta = ExecutorRegistration {
            id: "abc".to_owned(),
            optional_host: Some(OptionalHost::Host("http://localhost:8080".to_owned())),
            port: 0,
            grpc_port: 0,
            specification: Some(ExecutorSpecification { task_slots: 2 }.into()),
        };

        let request: Request<RegisterExecutorParams> =
            Request::new(RegisterExecutorParams {
                metadata: Some(exec_meta.clone()),
            });
        let response = scheduler
            .register_executor(request)
            .await
            .expect("Received error response")
            .into_inner();

        // registration should success
        assert!(response.success);

        let state = scheduler.state.clone();
        // executor should be registered
        let stored_executor = state
            .executor_manager
            .get_executor_metadata("abc")
            .await
            .expect("getting executor");

        assert_eq!(stored_executor.grpc_port, 0);
        assert_eq!(stored_executor.port, 0);
        assert_eq!(stored_executor.specification.task_slots, 2);
        assert_eq!(stored_executor.host, "http://localhost:8080".to_owned());

        // heartbeat from the executor
        let request: Request<HeartBeatParams> = Request::new(HeartBeatParams {
            executor_id: "abc".to_owned(),
            metrics: vec![],
            status: Some(ExecutorStatus {
                status: Some(executor_status::Status::Active("".to_string())),
            }),
            metadata: Some(exec_meta.clone()),
        });

        let _response = scheduler
            .heart_beat_from_executor(request)
            .await
            .expect("Received error response")
            .into_inner();

        let active_executors = state
            .executor_manager
            .get_alive_executors_within_one_minute();
        assert_eq!(active_executors.len(), 1);

        let expired_executors = state
            .executor_manager
            .get_expired_executors(scheduler.executor_termination_grace_period);
        assert!(expired_executors.is_empty());

        // simulate the heartbeat timeout
        tokio::time::sleep(Duration::from_secs(DEFAULT_EXECUTOR_TIMEOUT_SECONDS)).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        // executor should be marked to dead
        assert!(state.executor_manager.is_dead_executor("abc"));

        let active_executors = state
            .executor_manager
            .get_alive_executors_within_one_minute();
        assert!(active_executors.is_empty());
        Ok(())
    }
}
