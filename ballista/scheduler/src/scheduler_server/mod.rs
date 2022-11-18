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

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ballista_core::error::Result;
use ballista_core::event_loop::{EventLoop, EventSender};
use ballista_core::serde::protobuf::{StopExecutorParams, TaskStatus};
use ballista_core::serde::{AsExecutionPlan, BallistaCodec};

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;

use crate::config::SchedulerConfig;
use crate::metrics::SchedulerMetricsCollector;
use ballista_core::utils::SessionBuilder;
use log::{error, warn};

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::query_stage_scheduler::QueryStageScheduler;
use crate::state::backend::StateBackendClient;
use crate::state::executor_manager::{
    ExecutorManager, ExecutorReservation, DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
};

use crate::state::task_manager::TaskLauncher;
use crate::state::SchedulerState;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod externalscaler {
    include!(concat!(env!("OUT_DIR"), "/externalscaler.rs"));
}

pub mod event;
mod external_scaler;
mod grpc;
mod query_stage_scheduler;

#[derive(Clone)]
pub struct SchedulerServer<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    pub scheduler_name: String,
    pub start_time: u128,
    pub(crate) state: Arc<SchedulerState<T, U>>,
    pub(crate) query_stage_event_loop: EventLoop<QueryStageSchedulerEvent>,
    query_stage_scheduler: Arc<QueryStageScheduler<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerServer<T, U> {
    pub fn new(
        scheduler_name: String,
        config_backend: Arc<dyn StateBackendClient>,
        codec: BallistaCodec<T, U>,
        session_builder: Arc<dyn SessionBuilder>,
        config: SchedulerConfig,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
    ) -> Self {
        let state = Arc::new(SchedulerState::new(
            config_backend,
            session_builder,
            codec,
            scheduler_name.clone(),
            config.clone(),
        ));
        let query_stage_scheduler =
            Arc::new(QueryStageScheduler::new(state.clone(), metrics_collector));
        let query_stage_event_loop = EventLoop::new(
            "query_stage".to_owned(),
            config.event_loop_buffer_size as usize,
            query_stage_scheduler.clone(),
        );

        Self {
            scheduler_name,
            start_time: timestamp_millis() as u128,
            state,
            query_stage_event_loop,
            query_stage_scheduler,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_task_launcher(
        scheduler_name: String,
        config_backend: Arc<dyn StateBackendClient>,
        codec: BallistaCodec<T, U>,
        session_builder: Arc<dyn SessionBuilder>,
        config: SchedulerConfig,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
        task_launcher: Arc<dyn TaskLauncher>,
    ) -> Self {
        let state = Arc::new(SchedulerState::with_task_launcher(
            config_backend,
            session_builder,
            codec,
            scheduler_name.clone(),
            config.clone(),
            task_launcher,
        ));
        let query_stage_scheduler =
            Arc::new(QueryStageScheduler::new(state.clone(), metrics_collector));
        let query_stage_event_loop = EventLoop::new(
            "query_stage".to_owned(),
            config.event_loop_buffer_size as usize,
            query_stage_scheduler.clone(),
        );

        Self {
            scheduler_name,
            start_time: timestamp_millis() as u128,
            state,
            query_stage_event_loop,
            query_stage_scheduler,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        self.state.init().await?;
        self.query_stage_event_loop.start()?;
        self.expire_dead_executors()?;

        Ok(())
    }

    pub(crate) fn pending_tasks(&self) -> usize {
        self.query_stage_scheduler.pending_tasks()
    }

    pub(crate) fn metrics_collector(&self) -> &dyn SchedulerMetricsCollector {
        self.query_stage_scheduler.metrics_collector()
    }

    pub(crate) async fn submit_job(
        &self,
        job_id: &str,
        job_name: &str,
        ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
    ) -> Result<()> {
        self.query_stage_event_loop
            .get_sender()?
            .post_event(QueryStageSchedulerEvent::JobQueued {
                job_id: job_id.to_owned(),
                job_name: job_name.to_owned(),
                session_ctx: ctx,
                plan: Box::new(plan.clone()),
                queued_at: timestamp_millis(),
            })
            .await
    }

    /// It just send task status update event to the channel,
    /// and will not guarantee the event processing completed after return
    pub(crate) async fn update_task_status(
        &self,
        executor_id: &str,
        tasks_status: Vec<TaskStatus>,
    ) -> Result<()> {
        // We might receive buggy task updates from dead executors.
        if self.state.executor_manager.is_dead_executor(executor_id) {
            let error_msg = format!(
                "Receive buggy tasks status from dead Executor {}, task status update ignored.",
                executor_id
            );
            warn!("{}", error_msg);
            return Ok(());
        }
        self.query_stage_event_loop
            .get_sender()?
            .post_event(QueryStageSchedulerEvent::TaskUpdating(
                executor_id.to_owned(),
                tasks_status,
            ))
            .await
    }

    pub(crate) async fn offer_reservation(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<()> {
        self.query_stage_event_loop
            .get_sender()?
            .post_event(QueryStageSchedulerEvent::ReservationOffering(reservations))
            .await
    }

    /// Spawn an async task which periodically check the active executors' status and
    /// expire the dead executors
    fn expire_dead_executors(&self) -> Result<()> {
        let state = self.state.clone();
        let event_sender = self.query_stage_event_loop.get_sender()?;
        tokio::task::spawn(async move {
            loop {
                let expired_executors = state.executor_manager.get_expired_executors();
                for expired in expired_executors {
                    let executor_id = expired.executor_id.clone();
                    let executor_manager = state.executor_manager.clone();
                    let stop_reason = format!(
                        "Executor {} heartbeat timed out after {}s",
                        executor_id.clone(),
                        DEFAULT_EXECUTOR_TIMEOUT_SECONDS
                    );
                    warn!("{}", stop_reason.clone());
                    let sender_clone = event_sender.clone();
                    Self::remove_executor(
                        executor_manager,
                        sender_clone,
                        &executor_id,
                        Some(stop_reason.clone()),
                    )
                    .await
                    .unwrap_or_else(|e| {
                        let msg = format!(
                            "Error to remove Executor in Scheduler due to {:?}",
                            e
                        );
                        error!("{}", msg);
                    });

                    match state.executor_manager.get_client(&executor_id).await {
                        Ok(mut client) => {
                            tokio::task::spawn(async move {
                                match client
                                    .stop_executor(StopExecutorParams {
                                        executor_id,
                                        reason: stop_reason,
                                        force: true,
                                    })
                                    .await
                                {
                                    Err(error) => {
                                        warn!(
                                            "Failed to send stop_executor rpc due to, {}",
                                            error
                                        );
                                    }
                                    Ok(_value) => {}
                                }
                            });
                        }
                        Err(_) => {
                            warn!("Executor is already dead, failed to connect to Executor {}", executor_id);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(DEFAULT_EXECUTOR_TIMEOUT_SECONDS))
                    .await;
            }
        });
        Ok(())
    }

    pub(crate) async fn remove_executor(
        executor_manager: ExecutorManager,
        event_sender: EventSender<QueryStageSchedulerEvent>,
        executor_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        // Update the executor manager immediately here
        executor_manager
            .remove_executor(executor_id, reason.clone())
            .await?;

        event_sender
            .post_event(QueryStageSchedulerEvent::ExecutorLost(
                executor_id.to_owned(),
                reason,
            ))
            .await?;
        Ok(())
    }
}

pub fn timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

pub fn timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

#[cfg(all(test, feature = "sled"))]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, sum, LogicalPlan};

    use datafusion::test_util::scan_empty;
    use datafusion_proto::protobuf::LogicalPlanNode;

    use ballista_core::config::{
        BallistaConfig, TaskSchedulingPolicy, BALLISTA_DEFAULT_SHUFFLE_PARTITIONS,
    };
    use ballista_core::error::Result;

    use crate::config::SchedulerConfig;

    use ballista_core::serde::protobuf::{
        failed_task, job_status, task_status, ExecutionError, FailedTask, JobStatus,
        MultiTaskDefinition, PhysicalPlanNode, ShuffleWritePartition, SuccessfulJob,
        SuccessfulTask, TaskId, TaskStatus,
    };
    use ballista_core::serde::scheduler::{
        ExecutorData, ExecutorMetadata, ExecutorSpecification,
    };
    use ballista_core::serde::BallistaCodec;
    use ballista_core::utils::DefaultSessionBuilder;

    use crate::scheduler_server::{timestamp_millis, SchedulerServer};
    use crate::state::backend::standalone::StandaloneClient;

    use crate::test_utils::{
        assert_completed_event, assert_failed_event, assert_no_submitted_event,
        assert_submitted_event, ExplodingTableProvider, SchedulerTest, TaskRunnerFn,
        TestMetricsCollector,
    };

    #[tokio::test]
    async fn test_pull_scheduling() -> Result<()> {
        let plan = test_plan();
        let task_slots = 4;

        let scheduler = test_scheduler(TaskSchedulingPolicy::PullStaged).await?;

        let executors = test_executors(task_slots);
        for (executor_metadata, executor_data) in executors {
            scheduler
                .state
                .executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        let config = test_session(task_slots);

        let ctx = scheduler
            .state
            .session_manager
            .create_session(&config)
            .await?;

        let job_id = "job";

        // Submit job
        scheduler
            .state
            .submit_job(job_id, "", ctx, &plan, 0)
            .await
            .expect("submitting plan");

        // Refresh the ExecutionGraph
        while let Some(graph) = scheduler
            .state
            .task_manager
            .get_active_execution_graph(job_id)
            .await
        {
            let task = {
                let mut graph = graph.write().await;
                graph.pop_next_task("executor-1")?
            };
            if let Some(task) = task {
                let mut partitions: Vec<ShuffleWritePartition> = vec![];

                let num_partitions = task
                    .output_partitioning
                    .map(|p| p.partition_count())
                    .unwrap_or(1);

                for partition_id in 0..num_partitions {
                    partitions.push(ShuffleWritePartition {
                        partition_id: partition_id as u64,
                        path: "some/path".to_string(),
                        num_batches: 1,
                        num_rows: 1,
                        num_bytes: 1,
                    })
                }

                // Complete the task
                let task_status = TaskStatus {
                    task_id: task.task_id as u32,
                    job_id: task.partition.job_id.clone(),
                    stage_id: task.partition.stage_id as u32,
                    stage_attempt_num: task.stage_attempt_num as u32,
                    partition_id: task.partition.partition_id as u32,
                    launch_time: 0,
                    start_exec_time: 0,
                    end_exec_time: 0,
                    metrics: vec![],
                    status: Some(task_status::Status::Successful(SuccessfulTask {
                        executor_id: "executor-1".to_owned(),
                        partitions,
                    })),
                };

                scheduler
                    .state
                    .update_task_statuses("executor-1", vec![task_status])
                    .await?;
            } else {
                break;
            }
        }

        let final_graph = scheduler
            .state
            .task_manager
            .get_active_execution_graph(job_id)
            .await
            .expect("Fail to find graph in the cache");

        let final_graph = final_graph.read().await;
        assert!(final_graph.is_successful());
        assert_eq!(final_graph.output_locations().len(), 4);

        for output_location in final_graph.output_locations() {
            assert_eq!(output_location.path, "some/path".to_owned());
            assert_eq!(output_location.executor_meta.host, "localhost1".to_owned())
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_push_scheduling() -> Result<()> {
        let plan = test_plan();

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            4,
            1,
            None,
        )
        .await?;

        let status = test.run("job", "", &plan).await.expect("running plan");

        match status.status {
            Some(job_status::Status::Successful(SuccessfulJob {
                partition_location,
            })) => {
                assert_eq!(partition_location.len(), 4);
            }
            other => {
                panic!("Expected success status but found {:?}", other);
            }
        }

        assert_submitted_event("job", &metrics_collector);
        assert_completed_event("job", &metrics_collector);

        Ok(())
    }

    // Simulate a task failure and ensure the job status is updated correctly
    #[tokio::test]
    async fn test_job_failure() -> Result<()> {
        let plan = test_plan();

        let runner = Arc::new(TaskRunnerFn::new(
            |_executor_id: String, task: MultiTaskDefinition| {
                let mut statuses = vec![];

                for TaskId {
                    task_id,
                    partition_id,
                    ..
                } in task.task_ids
                {
                    let timestamp = timestamp_millis();
                    statuses.push(TaskStatus {
                        task_id,
                        job_id: task.job_id.clone(),
                        stage_id: task.stage_id,
                        stage_attempt_num: task.stage_attempt_num,
                        partition_id,
                        launch_time: timestamp,
                        start_exec_time: timestamp,
                        end_exec_time: timestamp,
                        metrics: vec![],
                        status: Some(task_status::Status::Failed(FailedTask {
                            error: "ERROR".to_string(),
                            retryable: false,
                            count_to_failures: false,
                            failed_reason: Some(
                                failed_task::FailedReason::ExecutionError(
                                    ExecutionError {},
                                ),
                            ),
                        })),
                    });
                }

                statuses
            },
        ));

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            4,
            1,
            Some(runner),
        )
        .await?;

        let status = test.run("job", "", &plan).await.expect("running plan");

        assert!(
            matches!(
                status,
                JobStatus {
                    status: Some(job_status::Status::Failed(_))
                }
            ),
            "Expected job status to be failed but it was {:?}",
            status
        );

        assert_submitted_event("job", &metrics_collector);
        assert_failed_event("job", &metrics_collector);

        Ok(())
    }

    // If the physical planning fails, the job should be marked as failed.
    // Here we simulate a planning failure using ExplodingTableProvider to test this.
    #[tokio::test]
    async fn test_planning_failure() -> Result<()> {
        let metrics_collector = Arc::new(TestMetricsCollector::default());
        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            4,
            1,
            None,
        )
        .await?;

        let ctx = test.ctx().await?;

        ctx.register_table("explode", Arc::new(ExplodingTableProvider))?;

        let plan = ctx.sql("SELECT * FROM explode").await?.to_logical_plan()?;

        // This should fail when we try and create the physical plan
        let status = test.run("job", "", &plan).await?;

        assert!(
            matches!(
                status,
                JobStatus {
                    status: Some(job_status::Status::Failed(_))
                }
            ),
            "Expected job status to be failed but it was {:?}",
            status
        );

        assert_no_submitted_event("job", &metrics_collector);
        assert_failed_event("job", &metrics_collector);

        Ok(())
    }

    async fn test_scheduler(
        scheduling_policy: TaskSchedulingPolicy,
    ) -> Result<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
        let session_builder = Arc::new(DefaultSessionBuilder {});
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);
        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                "localhost:50050".to_owned(),
                state_storage.clone(),
                BallistaCodec::default(),
                session_builder,
                SchedulerConfig::default().with_scheduler_policy(scheduling_policy),
                Arc::new(TestMetricsCollector::default()),
            );
        scheduler.init().await?;

        Ok(scheduler)
    }

    fn test_executors(num_partitions: usize) -> Vec<(ExecutorMetadata, ExecutorData)> {
        let task_slots = (num_partitions as u32 + 1) / 2;

        vec![
            (
                ExecutorMetadata {
                    id: "executor-1".to_string(),
                    host: "localhost1".to_string(),
                    port: 8080,
                    grpc_port: 9090,
                    specification: ExecutorSpecification { task_slots },
                },
                ExecutorData {
                    executor_id: "executor-1".to_owned(),
                    total_task_slots: task_slots,
                    available_task_slots: task_slots,
                },
            ),
            (
                ExecutorMetadata {
                    id: "executor-2".to_string(),
                    host: "localhost2".to_string(),
                    port: 8080,
                    grpc_port: 9090,
                    specification: ExecutorSpecification {
                        task_slots: num_partitions as u32 - task_slots,
                    },
                },
                ExecutorData {
                    executor_id: "executor-2".to_owned(),
                    total_task_slots: num_partitions as u32 - task_slots,
                    available_task_slots: num_partitions as u32 - task_slots,
                },
            ),
        ]
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        scan_empty(None, &schema, Some(vec![0, 1]))
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_session(partitions: usize) -> BallistaConfig {
        BallistaConfig::builder()
            .set(
                BALLISTA_DEFAULT_SHUFFLE_PARTITIONS,
                format!("{}", partitions).as_str(),
            )
            .build()
            .expect("creating BallistaConfig")
    }
}
