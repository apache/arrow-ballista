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

use ballista_core::config::{BallistaConfig, TaskSchedulingPolicy};

use ballista_core::serde::protobuf::execute_query_params::{
    OptionalJobId, OptionalSessionId, Query,
};

use ballista_core::serde::protobuf::executor_registration::OptionalHost;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpc;
use ballista_core::serde::protobuf::{
    ExecuteQueryParams, ExecuteQueryResult, ExecutorHeartbeat, FileType,
    GetFileMetadataParams, GetFileMetadataResult, GetJobStatusParams, GetJobStatusResult,
    HeartBeatParams, HeartBeatResult, PollWorkParams, PollWorkResult,
    RegisterExecutorParams, RegisterExecutorResult, UpdateTaskStatusParams,
    UpdateTaskStatusResult,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use ballista_core::serde::AsExecutionPlan;

use datafusion::datafusion_data_access::object_store::{
    local::LocalFileSystem, ObjectStore,
};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion_proto::logical_plan::AsLogicalPlan;
use futures::TryStreamExt;
use log::{debug, error, info, trace, warn};

// use http_body::Body;
use std::convert::TryInto;
use std::ops::Deref;
use std::sync::Arc;

use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};

use crate::scheduler_server::event::{QueryStageSchedulerEvent, SchedulerServerEvent};
use crate::scheduler_server::SchedulerServer;
use crate::state::executor_manager::ExecutorReservation;

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerGrpc
    for SchedulerServer<T, U>
{
    async fn poll_work(
        &self,
        request: Request<PollWorkParams>,
    ) -> Result<Response<PollWorkResult>, Status> {
        if let TaskSchedulingPolicy::PushStaged = self.policy {
            error!("Poll work interface is not supported for push-based task scheduling");
            return Err(tonic::Status::failed_precondition(
                "Bad request because poll work is not supported for push-based task scheduling",
            ));
        }
        let remote_addr = request.remote_addr();
        if let PollWorkParams {
            metadata: Some(metadata),
            can_accept_task,
            task_status,
        } = request.into_inner()
        {
            debug!("Received poll_work request for {:?}", metadata);
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
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs(),
                state: None,
            };

            self.state
                .executor_manager
                .save_executor_metadata(metadata.clone())
                .await
                .map_err(|e| {
                    let msg = format!("Could not save executor metadata: {}", e);
                    error!("{}", msg);
                    Status::internal(msg)
                })?;

            self.state
                .executor_manager
                .save_executor_heartbeat(executor_heartbeat)
                .await
                .map_err(|e| {
                    let msg = format!("Could not save executor heartbeat: {}", e);
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

            // If executor can accept another task, try and find one.
            let next_task = if can_accept_task {
                let reservations =
                    vec![ExecutorReservation::new_free(metadata.id.clone())];
                if let Ok((mut assignments, _, _)) = self
                    .state
                    .task_manager
                    .fill_reservations(&reservations)
                    .await
                {
                    if let Some((_, task)) = assignments.pop() {
                        match self.state.task_manager.prepare_task_definition(task) {
                            Ok(task_definition) => Some(task_definition),
                            Err(e) => {
                                error!("Error preparing task definition: {:?}", e);
                                None
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            Ok(Response::new(PollWorkResult { task: next_task }))
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
            let executor_data = ExecutorData {
                executor_id: metadata.id.clone(),
                total_task_slots: metadata.specification.task_slots,
                available_task_slots: metadata.specification.task_slots,
            };

            if let Ok(Some(sender)) =
                self.event_loop.as_ref().map(|e| e.get_sender()).transpose()
            {
                // If we are using push-based scheduling then reserve this executors slots and send
                // them for scheduling tasks.
                let reservations = self
                    .state
                    .executor_manager
                    .register_executor(metadata, executor_data, true)
                    .await
                    .unwrap();

                sender
                    .post_event(SchedulerServerEvent::Offer(reservations))
                    .await
                    .unwrap();
            } else {
                // Otherwise just save the executor to state
                self.state
                    .executor_manager
                    .register_executor(metadata, executor_data, false)
                    .await
                    .unwrap();
            }

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
        let HeartBeatParams { executor_id, state } = request.into_inner();

        debug!("Received heart beat request for {:?}", executor_id);
        trace!("Related executor state is {:?}", state);
        let executor_heartbeat = ExecutorHeartbeat {
            executor_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            state,
        };
        self.state
            .executor_manager
            .save_executor_heartbeat(executor_heartbeat)
            .await
            .map_err(|e| {
                let msg = format!("Could not save executor heartbeat: {}", e);
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
        // TODO support multiple object stores
        let obj_store = Arc::new(LocalFileSystem {}) as Arc<dyn ObjectStore>;
        // TODO shouldn't this take a ListingOption object as input?

        let GetFileMetadataParams { path, file_type } = request.into_inner();

        let file_type: FileType = file_type.try_into().map_err(|e| {
            let msg = format!("Error reading request: {}", e);
            error!("{}", msg);
            tonic::Status::internal(msg)
        })?;

        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::Parquet => Ok(Arc::new(ParquetFormat::default())),
            // TODO implement for CSV
            _ => Err(tonic::Status::unimplemented(
                "get_file_metadata unsupported file type",
            )),
        }?;

        let file_metas: Vec<_> = obj_store
            .list_file(&path)
            .await
            .map_err(|e| {
                let msg = format!("Error listing files: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?
            .try_collect()
            .await?;

        let schema = file_format
            .infer_schema(&obj_store, &file_metas)
            .await
            .map_err(|e| {
                let msg = format!("Error inferring schema: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;

        Ok(Response::new(GetFileMetadataResult {
            schema: Some(schema.as_ref().into()),
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
            optional_job_id,
        } = query_params
        {
            // parse config
            let mut config_builder = BallistaConfig::builder();
            for kv_pair in &settings {
                config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
            }
            let config = config_builder.build().map_err(|e| {
                let msg = format!("Could not parse configs: {}", e);
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
                                "Failed to load SessionContext for session ID {}: {:?}",
                                session_id, e
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
                                "Failed to create SessionContext: {:?}",
                                e
                            ))
                        })?;

                    (ctx.session_id(), ctx)
                }
            };

            let plan = match query {
                Query::LogicalPlan(message) => T::try_decode(message.as_slice())
                    .and_then(|m| {
                        m.try_into_logical_plan(
                            session_ctx.deref(),
                            self.codec.logical_extension_codec(),
                        )
                    })
                    .map_err(|e| {
                        let msg = format!("Could not parse logical plan protobuf: {}", e);
                        error!("{}", msg);
                        Status::internal(msg)
                    })?,
                Query::Sql(sql) => session_ctx
                    .sql(&sql)
                    .await
                    .and_then(|df| df.to_logical_plan())
                    .map_err(|e| {
                        let msg = format!("Error parsing SQL: {}", e);
                        error!("{}", msg);
                        Status::internal(msg)
                    })?,
            };

            debug!("Received plan for execution: {:?}", plan);

            let job_id = match optional_job_id {
                Some(OptionalJobId::JobId(job_id)) => job_id,
                _ => self.state.task_manager.generate_job_id(),
            };

            self.state
                .task_manager
                .queue_job(&job_id)
                .await
                .map_err(|e| {
                    let msg = format!("Failed to queue job {}: {:?}", job_id, e);
                    error!("{}", msg);

                    Status::internal(msg)
                })?;

            let query_stage_event_sender =
                self.query_stage_event_loop.get_sender().map_err(|e| {
                    Status::internal(format!(
                        "Could not get query stage event sender due to: {}",
                        e
                    ))
                })?;

            query_stage_event_sender
                .post_event(QueryStageSchedulerEvent::JobQueued {
                    job_id: job_id.clone(),
                    session_id: session_id.clone(),
                    session_ctx,
                    plan: Box::new(plan),
                })
                .await
                .map_err(|e| {
                    let msg =
                        format!("Failed to send JobQueued event for {}: {:?}", job_id, e);
                    error!("{}", msg);

                    Status::internal(msg)
                })?;

            Ok(Response::new(ExecuteQueryResult { job_id, session_id }))
        } else if let ExecuteQueryParams {
            query: None,
            settings,
            optional_session_id: None,
            optional_job_id: None,
        } = query_params
        {
            // parse config for new session
            let mut config_builder = BallistaConfig::builder();
            for kv_pair in &settings {
                config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
            }
            let config = config_builder.build().map_err(|e| {
                let msg = format!("Could not parse configs: {}", e);
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
                        "Failed to create new SessionContext: {:?}",
                        e
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
        debug!("Received get_job_status request for job {}", job_id);
        match self.state.task_manager.get_job_status(&job_id).await {
            Ok(status) => Ok(Response::new(GetJobStatusResult {
                status: Some(status),
            })),
            Err(e) => {
                let msg = format!("Error getting status for job {}: {:?}", job_id, e);
                error!("{}", msg);
                Err(Status::internal(msg))
            }
        }
    }
}

#[cfg(all(test, feature = "sled"))]
mod test {
    use std::sync::Arc;

    use tonic::Request;

    use crate::state::{backend::standalone::StandaloneClient, SchedulerState};
    use ballista_core::error::BallistaError;
    use ballista_core::serde::protobuf::{
        executor_registration::OptionalHost, ExecutorRegistration, PhysicalPlanNode,
        PollWorkParams,
    };
    use ballista_core::serde::scheduler::ExecutorSpecification;
    use ballista_core::serde::BallistaCodec;
    use datafusion::execution::context::default_session_builder;
    use datafusion_proto::protobuf::LogicalPlanNode;

    use super::{SchedulerGrpc, SchedulerServer};

    #[tokio::test]
    async fn test_poll_work() -> Result<(), BallistaError> {
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);
        let namespace = "default";
        let scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                state_storage.clone(),
                namespace.to_owned(),
                BallistaCodec::default(),
            );
        let exec_meta = ExecutorRegistration {
            id: "abc".to_owned(),
            optional_host: Some(OptionalHost::Host("http://host:8080".to_owned())),
            port: 0,
            grpc_port: 0,
            specification: Some(ExecutorSpecification { task_slots: 2 }.into()),
        };
        let request: Request<PollWorkParams> = Request::new(PollWorkParams {
            metadata: Some(exec_meta.clone()),
            can_accept_task: false,
            task_status: vec![],
        });
        let response = scheduler
            .poll_work(request)
            .await
            .expect("Received error response")
            .into_inner();
        // no response task since we told the scheduler we didn't want to accept one
        assert!(response.task.is_none());
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                state_storage.clone(),
                namespace.to_string(),
                default_session_builder,
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
        assert_eq!(stored_executor.host, "http://host:8080".to_owned());

        let request: Request<PollWorkParams> = Request::new(PollWorkParams {
            metadata: Some(exec_meta.clone()),
            can_accept_task: true,
            task_status: vec![],
        });
        let response = scheduler
            .poll_work(request)
            .await
            .expect("Received error response")
            .into_inner();

        // still no response task since there are no tasks in the scheduler
        assert!(response.task.is_none());
        let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerState::new(
                state_storage.clone(),
                namespace.to_string(),
                default_session_builder,
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
        assert_eq!(stored_executor.host, "http://host:8080".to_owned());

        Ok(())
    }
}
