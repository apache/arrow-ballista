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

use datafusion::common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion::datasource::listing::{ListingTable, ListingTableUrl};
use datafusion::datasource::source_as_provider;
use datafusion::error::DataFusionError;
use std::any::type_name;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::scheduler_server::event::QueryStageSchedulerEvent;

use crate::state::executor_manager::ExecutorManager;
use crate::state::session_manager::SessionManager;
use crate::state::task_manager::{TaskLauncher, TaskManager};

use crate::cluster::{BallistaCluster, BoundTask, ExecutorSlot};
use crate::config::SchedulerConfig;
use crate::state::execution_graph::TaskDescription;
use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::EventSender;
use ballista_core::serde::protobuf::TaskStatus;
use ballista_core::serde::BallistaCodec;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use log::{debug, error, info, warn};
use prost::Message;

pub mod execution_graph;
pub mod execution_graph_dot;
pub mod executor_manager;
pub mod session_manager;
pub mod task_manager;

pub fn decode_protobuf<T: Message + Default>(bytes: &[u8]) -> Result<T> {
    T::decode(bytes).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not deserialize {}: {}",
            type_name::<T>(),
            e
        ))
    })
}

pub fn decode_into<T: Message + Default + Into<U>, U>(bytes: &[u8]) -> Result<U> {
    T::decode(bytes)
        .map_err(|e| {
            BallistaError::Internal(format!(
                "Could not deserialize {}: {}",
                type_name::<T>(),
                e
            ))
        })
        .map(|t| t.into())
}

pub fn encode_protobuf<T: Message + Default>(msg: &T) -> Result<Vec<u8>> {
    let mut value: Vec<u8> = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut value).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not serialize {}: {}",
            type_name::<T>(),
            e
        ))
    })?;
    Ok(value)
}

#[derive(Clone)]
pub struct SchedulerState<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    pub executor_manager: ExecutorManager,
    pub task_manager: TaskManager<T, U>,
    pub session_manager: SessionManager,
    pub codec: BallistaCodec<T, U>,
    pub config: Arc<SchedulerConfig>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerState<T, U> {
    pub fn new(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
        scheduler_name: String,
        config: Arc<SchedulerConfig>,
    ) -> Self {
        Self {
            executor_manager: ExecutorManager::new(
                cluster.cluster_state(),
                config.clone(),
            ),
            task_manager: TaskManager::new(
                cluster.job_state(),
                codec.clone(),
                scheduler_name,
            ),
            session_manager: SessionManager::new(cluster.job_state()),
            codec,
            config,
        }
    }

    #[cfg(test)]
    pub fn new_with_default_scheduler_name(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        let config = Arc::new(SchedulerConfig::default());
        SchedulerState::new(cluster, codec, "localhost:50050".to_owned(), config)
    }

    #[allow(dead_code)]
    pub(crate) fn new_with_task_launcher(
        cluster: BallistaCluster,
        codec: BallistaCodec<T, U>,
        scheduler_name: String,
        config: Arc<SchedulerConfig>,
        dispatcher: Arc<dyn TaskLauncher>,
    ) -> Self {
        Self {
            executor_manager: ExecutorManager::new(
                cluster.cluster_state(),
                config.clone(),
            ),
            task_manager: TaskManager::with_launcher(
                cluster.job_state(),
                codec.clone(),
                scheduler_name,
                dispatcher,
            ),
            session_manager: SessionManager::new(cluster.job_state()),
            codec,
            config,
        }
    }

    pub async fn init(&self) -> Result<()> {
        self.executor_manager.init().await
    }

    pub(crate) async fn revive_offers(
        &self,
        sender: EventSender<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let schedulable_tasks = self
            .executor_manager
            .bind_schedulable_tasks(self.task_manager.get_running_job_cache())
            .await?;
        if schedulable_tasks.is_empty() {
            warn!("No schedulable tasks found to be launched");
            return Ok(());
        }

        let state = self.clone();
        tokio::spawn(async move {
            let mut if_revive = false;
            match state.launch_tasks(schedulable_tasks).await {
                Ok(unassigned_executor_slots) => {
                    if !unassigned_executor_slots.is_empty() {
                        if let Err(e) = state
                            .executor_manager
                            .unbind_tasks(unassigned_executor_slots)
                            .await
                        {
                            error!("Fail to unbind tasks: {}", e);
                        }
                        if_revive = true;
                    }
                }
                Err(e) => {
                    error!("Fail to launch tasks: {}", e);
                    if_revive = true;
                }
            }
            if if_revive {
                if let Err(e) = sender
                    .post_event(QueryStageSchedulerEvent::ReviveOffers)
                    .await
                {
                    error!("Fail to send revive offers event due to {:?}", e);
                }
            }
        });

        Ok(())
    }

    /// Remove an executor.
    /// 1. The executor related info will be removed from [`ExecutorManager`]
    /// 2. All of affected running execution graph will be rolled backed
    /// 3. All of the running tasks of the affected running stages will be cancelled
    pub(crate) async fn remove_executor(
        &self,
        executor_id: &str,
        reason: Option<String>,
    ) {
        if let Err(e) = self
            .executor_manager
            .remove_executor(executor_id, reason)
            .await
        {
            warn!("Fail to remove executor {}: {}", executor_id, e);
        }

        match self.task_manager.executor_lost(executor_id).await {
            Ok(tasks) => {
                if !tasks.is_empty() {
                    if let Err(e) =
                        self.executor_manager.cancel_running_tasks(tasks).await
                    {
                        warn!("Fail to cancel running tasks due to {:?}", e);
                    }
                }
            }
            Err(e) => {
                error!(
                    "TaskManager error to handle Executor {} lost: {}",
                    executor_id, e
                );
            }
        }
    }

    /// Given a vector of bound tasks,
    /// 1. Firstly reorganize according to: executor -> job stage -> tasks;
    /// 2. Then launch the task set vector to each executor one by one.
    ///
    /// If it fails to launch a task set, the related [`ExecutorSlot`] will be returned.
    async fn launch_tasks(
        &self,
        bound_tasks: Vec<BoundTask>,
    ) -> Result<Vec<ExecutorSlot>> {
        // Put tasks to the same executor together
        // And put tasks belonging to the same stage together for creating MultiTaskDefinition
        let mut executor_stage_assignments: HashMap<
            String,
            HashMap<(String, usize), Vec<TaskDescription>>,
        > = HashMap::new();
        for (executor_id, task) in bound_tasks.into_iter() {
            let stage_key = (task.partition.job_id.clone(), task.partition.stage_id);
            if let Some(tasks) = executor_stage_assignments.get_mut(&executor_id) {
                if let Some(executor_stage_tasks) = tasks.get_mut(&stage_key) {
                    executor_stage_tasks.push(task);
                } else {
                    tasks.insert(stage_key, vec![task]);
                }
            } else {
                let mut executor_stage_tasks: HashMap<
                    (String, usize),
                    Vec<TaskDescription>,
                > = HashMap::new();
                executor_stage_tasks.insert(stage_key, vec![task]);
                executor_stage_assignments.insert(executor_id, executor_stage_tasks);
            }
        }

        let mut join_handles = vec![];
        for (executor_id, tasks) in executor_stage_assignments.into_iter() {
            let tasks: Vec<Vec<TaskDescription>> = tasks.into_values().collect();
            // Total number of tasks to be launched for one executor
            let n_tasks: usize = tasks.iter().map(|stage_tasks| stage_tasks.len()).sum();

            let state = self.clone();
            let join_handle = tokio::spawn(async move {
                let success = match state
                    .executor_manager
                    .get_executor_metadata(&executor_id)
                    .await
                {
                    Ok(executor) => {
                        if let Err(e) = state
                            .task_manager
                            .launch_multi_task(&executor, tasks, &state.executor_manager)
                            .await
                        {
                            let err_msg = format!("Failed to launch new task: {e}");
                            error!("{}", err_msg.clone());

                            // It's OK to remove executor aggressively,
                            // since if the executor is in healthy state, it will be registered again.
                            state.remove_executor(&executor_id, Some(err_msg)).await;

                            false
                        } else {
                            true
                        }
                    }
                    Err(e) => {
                        error!("Failed to launch new task, could not get executor metadata: {}", e);
                        false
                    }
                };
                if success {
                    vec![]
                } else {
                    vec![(executor_id.clone(), n_tasks as u32)]
                }
            });
            join_handles.push(join_handle);
        }

        let unassigned_executor_slots =
            futures::future::join_all(join_handles)
                .await
                .into_iter()
                .collect::<std::result::Result<
                    Vec<Vec<ExecutorSlot>>,
                    tokio::task::JoinError,
                >>()?;

        Ok(unassigned_executor_slots
            .into_iter()
            .flatten()
            .collect::<Vec<ExecutorSlot>>())
    }

    pub(crate) async fn update_task_statuses(
        &self,
        executor_id: &str,
        tasks_status: Vec<TaskStatus>,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let executor = self
            .executor_manager
            .get_executor_metadata(executor_id)
            .await?;

        self.task_manager
            .update_task_statuses(&executor, tasks_status)
            .await
    }

    pub(crate) async fn submit_job(
        &self,
        job_id: &str,
        job_name: &str,
        session_ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
        queued_at: u64,
    ) -> Result<()> {
        let start = Instant::now();

        if log::max_level() >= log::Level::Debug {
            // optimizing the plan here is redundant because the physical planner will do this again
            // but it is helpful to see what the optimized plan will be
            let optimized_plan = session_ctx.state().optimize(plan)?;
            debug!("Optimized plan: {}", optimized_plan.display_indent());
        }

        plan.apply(&mut |plan| {
            if let LogicalPlan::TableScan(scan) = plan {
                let provider = source_as_provider(&scan.source)?;
                if let Some(table) = provider.as_any().downcast_ref::<ListingTable>() {
                    let local_paths: Vec<&ListingTableUrl> = table
                        .table_paths()
                        .iter()
                        .filter(|url| url.as_str().starts_with("file:///"))
                        .collect();
                    if !local_paths.is_empty() {
                        // These are local files rather than remote object stores, so we
                        // need to check that they are accessible on the scheduler (the client
                        // may not be on the same host, or the data path may not be correctly
                        // mounted in the container). There could be thousands of files so we
                        // just check the first one.
                        let url = &local_paths[0].as_str();
                        // the unwraps are safe here because we checked that the url starts with file:///
                        // we need to check both versions here to support Linux & Windows
                        ListingTableUrl::parse(url.strip_prefix("file://").unwrap())
                            .or_else(|_| {
                                ListingTableUrl::parse(
                                    url.strip_prefix("file:///").unwrap(),
                                )
                            })
                            .map_err(|e| {
                                DataFusionError::External(
                                    format!(
                                        "logical plan refers to path on local file system \
                                that is not accessible in the scheduler: {url}: {e:?}"
                                    )
                                        .into(),
                                )
                            })?;
                    }
                }
            }
            Ok(VisitRecursion::Continue)
        })?;

        let plan = session_ctx.state().create_physical_plan(plan).await?;
        debug!(
            "Physical plan: {}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(false)
        );

        let plan = plan.transform_down(&|node| {
            if node.output_partitioning().partition_count() == 0 {
                Ok(Transformed::Yes(Arc::new(EmptyExec::new(node.schema()))))
            } else {
                Ok(Transformed::No(node))
            }
        })?;
        debug!(
            "Transformed physical plan: {}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(false)
        );

        self.task_manager
            .submit_job(job_id, job_name, &session_ctx.session_id(), plan, queued_at)
            .await?;

        let elapsed = start.elapsed();

        info!("Planned job {} in {:?}", job_id, elapsed);

        Ok(())
    }

    /// Spawn a delayed future to clean up job data on both Scheduler and Executors
    pub(crate) fn clean_up_successful_job(&self, job_id: String) {
        self.executor_manager.clean_up_job_data_delayed(
            job_id.clone(),
            self.config.finished_job_data_clean_up_interval_seconds,
        );
        self.task_manager.clean_up_job_delayed(
            job_id,
            self.config.finished_job_state_clean_up_interval_seconds,
        );
    }

    /// Spawn a delayed future to clean up job data on both Scheduler and Executors
    pub(crate) fn clean_up_failed_job(&self, job_id: String) {
        self.executor_manager.clean_up_job_data(job_id.clone());
        self.task_manager.clean_up_job_delayed(
            job_id,
            self.config.finished_job_state_clean_up_interval_seconds,
        );
    }
}
