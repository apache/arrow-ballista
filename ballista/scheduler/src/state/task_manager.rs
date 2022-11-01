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

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::state::backend::{Keyspace, Operation, StateBackendClient};
use crate::state::execution_graph::{
    ExecutionGraph, ExecutionStage, RunningTaskInfo, TaskDescription,
};
use crate::state::executor_manager::{ExecutorManager, ExecutorReservation};
use crate::state::{decode_protobuf, encode_protobuf, with_lock, with_locks};
use ballista_core::config::BallistaConfig;
use ballista_core::error::BallistaError;
use ballista_core::error::Result;

use crate::state::session_manager::create_datafusion_context;
use ballista_core::serde::protobuf::{
    self, job_status, FailedJob, JobStatus, MultiTaskDefinition, TaskDefinition, TaskId,
    TaskStatus,
};
use ballista_core::serde::scheduler::to_proto::hash_partitioning_to_proto;
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_core::serde::{AsExecutionPlan, BallistaCodec};
use ballista_core::utils::SessionBuilder;
use dashmap::DashMap;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use log::{debug, error, info, warn};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

use tracing::trace;

type ActiveJobCache = Arc<DashMap<String, JobInfoCache>>;

// TODO move to configuration file
/// Default max failure attempts for task level retry
pub const TASK_MAX_FAILURES: usize = 4;
/// Default max failure attempts for stage level retry
pub const STAGE_MAX_FAILURES: usize = 4;

#[derive(Clone)]
pub struct TaskManager<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    state: Arc<dyn StateBackendClient>,
    session_builder: Arc<dyn SessionBuilder>,
    codec: BallistaCodec<T, U>,
    scheduler_id: String,
    // Cache for active jobs curated by this scheduler
    active_job_cache: ActiveJobCache,
}

#[derive(Clone)]
struct JobInfoCache {
    // Cache for active execution graphs curated by this scheduler
    execution_graph: Arc<RwLock<ExecutionGraph>>,
    // Cache for encoded execution stage plan to avoid duplicated encoding for multiple tasks
    encoded_stage_plans: HashMap<usize, Vec<u8>>,
}

impl JobInfoCache {
    fn new(graph: ExecutionGraph) -> Self {
        Self {
            execution_graph: Arc::new(RwLock::new(graph)),
            encoded_stage_plans: HashMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct UpdatedStages {
    pub resolved_stages: HashSet<usize>,
    pub successful_stages: HashSet<usize>,
    pub failed_stages: HashMap<usize, String>,
    pub rollback_running_stages: HashMap<usize, HashSet<String>>,
    pub resubmit_successful_stages: HashSet<usize>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskManager<T, U> {
    pub fn new(
        state: Arc<dyn StateBackendClient>,
        session_builder: Arc<dyn SessionBuilder>,
        codec: BallistaCodec<T, U>,
        scheduler_id: String,
    ) -> Self {
        Self {
            state,
            session_builder,
            codec,
            scheduler_id,
            active_job_cache: Arc::new(DashMap::new()),
        }
    }

    /// Generate an ExecutionGraph for the job and save it to the persistent state.
    /// By default, this job will be curated by the scheduler which receives it.
    /// Then we will also save it to the active execution graph
    pub async fn submit_job(
        &self,
        job_id: &str,
        job_name: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        let mut graph =
            ExecutionGraph::new(&self.scheduler_id, job_id, job_name, session_id, plan)?;
        info!("Submitting execution graph: {:?}", graph);
        self.state
            .put(
                Keyspace::ActiveJobs,
                job_id.to_owned(),
                self.encode_execution_graph(graph.clone())?,
            )
            .await?;

        graph.revive();
        self.active_job_cache
            .insert(job_id.to_owned(), JobInfoCache::new(graph));

        Ok(())
    }

    /// Get a list of active job ids
    pub async fn get_jobs(&self) -> Result<Vec<JobOverview>> {
        let mut job_ids = vec![];
        for job_id in self.state.scan_keys(Keyspace::ActiveJobs).await? {
            job_ids.push(job_id);
        }
        for job_id in self.state.scan_keys(Keyspace::CompletedJobs).await? {
            job_ids.push(job_id);
        }
        for job_id in self.state.scan_keys(Keyspace::FailedJobs).await? {
            job_ids.push(job_id);
        }

        let mut jobs = vec![];
        for job_id in &job_ids {
            let graph = self.get_execution_graph(job_id).await?;

            let mut completed_stages = 0;
            for stage in graph.stages().values() {
                if let ExecutionStage::Successful(_) = stage {
                    completed_stages += 1;
                }
            }
            jobs.push(JobOverview {
                job_id: job_id.clone(),
                job_name: graph.job_name().to_string(),
                status: graph.status(),
                start_time: graph.start_time(),
                end_time: graph.end_time(),
                num_stages: graph.stage_count(),
                completed_stages,
            });
        }
        Ok(jobs)
    }

    /// Get the status of of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs, and then in Failed jobs
    pub async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let status = graph.read().await.status();
            Ok(Some(status))
        } else if let Ok(graph) = self.get_execution_graph(job_id).await {
            Ok(Some(graph.status()))
        } else {
            let value = self.state.get(Keyspace::FailedJobs, job_id).await?;

            if !value.is_empty() {
                let status = decode_protobuf(&value)?;
                Ok(Some(status))
            } else {
                Ok(None)
            }
        }
    }

    /// Get the execution graph of of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs.
    pub async fn get_job_execution_graph(
        &self,
        job_id: &str,
    ) -> Result<Option<Arc<ExecutionGraph>>> {
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            Ok(Some(Arc::new(graph.read().await.clone())))
        } else if let Ok(graph) = self.get_execution_graph(job_id).await {
            Ok(Some(Arc::new(graph)))
        } else {
            // if the job failed then we return no graph for now
            Ok(None)
        }
    }

    /// Update given task statuses in the respective job and return a tuple containing:
    /// 1. A list of QueryStageSchedulerEvent to publish.
    /// 2. A list of reservations that can now be offered.
    pub(crate) async fn update_task_statuses(
        &self,
        executor: &ExecutorMetadata,
        task_status: Vec<TaskStatus>,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let mut job_updates: HashMap<String, Vec<TaskStatus>> = HashMap::new();
        for status in task_status {
            trace!("Task Update\n{:?}", status);
            let job_id = status.job_id.clone();
            let job_task_statuses = job_updates.entry(job_id).or_insert_with(Vec::new);
            job_task_statuses.push(status);
        }

        let mut events: Vec<QueryStageSchedulerEvent> = vec![];
        for (job_id, statuses) in job_updates {
            let num_tasks = statuses.len();
            debug!("Updating {} tasks in job {}", num_tasks, job_id);

            let graph = self.get_active_execution_graph(&job_id).await;
            let job_events = if let Some(graph) = graph {
                let mut graph = graph.write().await;
                graph.update_task_status(
                    executor,
                    statuses,
                    TASK_MAX_FAILURES,
                    STAGE_MAX_FAILURES,
                )?
            } else {
                // TODO Deal with curator changed case
                error!("Fail to find job {} in the active cache and it may not be curated by this scheduler", job_id);
                vec![]
            };

            for event in job_events {
                events.push(event);
            }
        }

        Ok(events)
    }

    /// Take a list of executor reservations and fill them with tasks that are ready
    /// to be scheduled.
    ///
    /// Here we use the following  algorithm:
    ///
    /// 1. For each free reservation, try to assign a task from one of the active jobs
    /// 2. If we cannot find a task in all active jobs, then add the reservation to the list of unassigned reservations
    ///
    /// Finally, we return:
    /// 1. A list of assignments which is a (Executor ID, Task) tuple
    /// 2. A list of unassigned reservations which we could not find tasks for
    /// 3. The number of pending tasks across active jobs
    pub async fn fill_reservations(
        &self,
        reservations: &[ExecutorReservation],
    ) -> Result<(
        Vec<(String, TaskDescription)>,
        Vec<ExecutorReservation>,
        usize,
    )> {
        // Reinitialize the free reservations.
        let free_reservations: Vec<ExecutorReservation> = reservations
            .iter()
            .map(|reservation| {
                ExecutorReservation::new_free(reservation.executor_id.clone())
            })
            .collect();

        let mut assignments: Vec<(String, TaskDescription)> = vec![];
        let mut pending_tasks = 0usize;
        let mut assign_tasks = 0usize;
        for pairs in self.active_job_cache.iter() {
            let (_job_id, job_info) = pairs.pair();
            let mut graph = job_info.execution_graph.write().await;
            for reservation in free_reservations.iter().skip(assign_tasks) {
                if let Some(task) = graph.pop_next_task(&reservation.executor_id)? {
                    assignments.push((reservation.executor_id.clone(), task));
                    assign_tasks += 1;
                } else {
                    break;
                }
            }
            if assign_tasks >= free_reservations.len() {
                pending_tasks = graph.available_tasks();
                break;
            }
        }

        let mut unassigned = vec![];
        for reservation in free_reservations.iter().skip(assign_tasks) {
            unassigned.push(reservation.clone());
        }
        Ok((assignments, unassigned, pending_tasks))
    }

    /// Mark a job to success. This will create a key under the CompletedJobs keyspace
    /// and remove the job from ActiveJobs
    pub(crate) async fn succeed_job(
        &self,
        job_id: &str,
        clean_up_interval: u64,
    ) -> Result<()> {
        debug!("Moving job {} from Active to Success", job_id);
        let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;
        with_lock(lock, self.state.delete(Keyspace::ActiveJobs, job_id)).await?;

        if let Some(graph) = self.remove_active_execution_graph(job_id).await {
            let graph = graph.read().await.clone();
            if graph.is_successful() {
                let value = self.encode_execution_graph(graph)?;
                self.state
                    .put(Keyspace::CompletedJobs, job_id.to_owned(), value)
                    .await?;
            } else {
                error!("Job {} has not finished and cannot be completed", job_id);
                return Ok(());
            }
        } else {
            warn!("Fail to find job {} in the cache", job_id);
        }

        // spawn a delayed future to clean up job data on both Scheduler and Executors
        let state = self.state.clone();
        let job_id_str = job_id.to_owned();
        let active_job_cache = self.active_job_cache.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(clean_up_interval)).await;
            Self::clean_up_job_data(state, active_job_cache, false, job_id_str).await
        });

        Ok(())
    }

    /// Cancel the job and return a Vec of running tasks need to cancel
    pub(crate) async fn cancel_job(
        &self,
        job_id: &str,
        clean_up_interval_in_secs: u64,
    ) -> Result<Vec<RunningTaskInfo>> {
        self.abort_job(job_id, "Cancelled".to_owned(), clean_up_interval_in_secs)
            .await
    }

    /// Abort the job and return a Vec of running tasks need to cancel
    pub(crate) async fn abort_job(
        &self,
        job_id: &str,
        failure_reason: String,
        clean_up_interval_in_secs: u64,
    ) -> Result<Vec<RunningTaskInfo>> {
        let locks = self
            .state
            .acquire_locks(vec![
                (Keyspace::ActiveJobs, job_id),
                (Keyspace::FailedJobs, job_id),
            ])
            .await?;
        let tasks_to_cancel = if let Some(graph) =
            self.get_active_execution_graph(job_id).await
        {
            let running_tasks = graph.read().await.running_tasks();
            info!(
                "Cancelling {} running tasks for job {}",
                running_tasks.len(),
                job_id
            );
            with_locks(locks, self.fail_job_state(job_id, failure_reason)).await?;
            running_tasks
        } else {
            // TODO listen the job state update event and fix task cancelling
            warn!("Fail to find job {} in the cache, unable to cancel tasks for job, fail the job state only.", job_id);
            with_locks(locks, self.fail_job_state(job_id, failure_reason)).await?;
            vec![]
        };

        // spawn a delayed future to clean up job data on both Scheduler and Executors
        let state = self.state.clone();
        let job_id_str = job_id.to_owned();
        let active_job_cache = self.active_job_cache.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(clean_up_interval_in_secs)).await;
            Self::clean_up_job_data(state, active_job_cache, true, job_id_str).await
        });

        Ok(tasks_to_cancel)
    }

    /// Mark a unscheduled job as failed. This will create a key under the FailedJobs keyspace
    /// and remove the job from ActiveJobs or QueuedJobs
    pub async fn fail_unscheduled_job(
        &self,
        job_id: &str,
        failure_reason: String,
        clean_up_interval_in_secs: u64,
    ) -> Result<()> {
        debug!("Moving job {} from Active or Queue to Failed", job_id);
        let locks = self
            .state
            .acquire_locks(vec![
                (Keyspace::ActiveJobs, job_id),
                (Keyspace::FailedJobs, job_id),
            ])
            .await?;
        with_locks(locks, self.fail_job_state(job_id, failure_reason)).await?;

        // spawn a delayed future to clean up job data on Scheduler
        let state = self.state.clone();
        let job_id_str = job_id.to_owned();
        let active_job_cache = self.active_job_cache.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(clean_up_interval_in_secs)).await;
            Self::clean_up_job_data(state, active_job_cache, true, job_id_str).await
        });

        Ok(())
    }

    async fn fail_job_state(&self, job_id: &str, failure_reason: String) -> Result<()> {
        let txn_operations = |value: Vec<u8>| -> Vec<(Operation, Keyspace, String)> {
            vec![
                (Operation::Delete, Keyspace::ActiveJobs, job_id.to_string()),
                (
                    Operation::Put(value),
                    Keyspace::FailedJobs,
                    job_id.to_string(),
                ),
            ]
        };

        let _res = if let Some(graph) = self.remove_active_execution_graph(job_id).await {
            let mut graph = graph.write().await;
            let previous_status = graph.status();
            graph.fail_job(failure_reason);
            let value = self.encode_execution_graph(graph.clone())?;
            let txn_ops = txn_operations(value);
            let result = self.state.apply_txn(txn_ops).await;
            if result.is_err() {
                // Rollback
                graph.update_status(previous_status);
                warn!("Rollback Execution Graph state change since it did not persisted due to a possible connection error.")
            };
            result
        } else {
            info!("Fail to find job {} in the cache", job_id);
            let status = JobStatus {
                status: Some(job_status::Status::Failed(FailedJob {
                    error: failure_reason.clone(),
                })),
            };
            let value = encode_protobuf(&status)?;
            let txn_ops = txn_operations(value);
            self.state.apply_txn(txn_ops).await
        };

        Ok(())
    }

    pub async fn update_job(&self, job_id: &str) -> Result<()> {
        debug!("Update job {} in Active", job_id);
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let mut graph = graph.write().await;
            graph.revive();
            let graph = graph.clone();
            let value = self.encode_execution_graph(graph)?;
            self.state
                .put(Keyspace::ActiveJobs, job_id.to_owned(), value)
                .await?;
        } else {
            warn!("Fail to find job {} in the cache", job_id);
        }

        Ok(())
    }

    /// return a Vec of running tasks need to cancel
    pub async fn executor_lost(&self, executor_id: &str) -> Result<Vec<RunningTaskInfo>> {
        // Collect all the running task need to cancel when there are running stages rolled back.
        let mut running_tasks_to_cancel: Vec<RunningTaskInfo> = vec![];
        // Collect graphs we update so we can update them in storage
        let updated_graphs: DashMap<String, ExecutionGraph> = DashMap::new();
        {
            for pairs in self.active_job_cache.iter() {
                let (job_id, job_info) = pairs.pair();
                let mut graph = job_info.execution_graph.write().await;
                let reset = graph.reset_stages_on_lost_executor(executor_id)?;
                if !reset.0.is_empty() {
                    updated_graphs.insert(job_id.to_owned(), graph.clone());
                    running_tasks_to_cancel.extend(reset.1);
                }
            }
        }

        let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;
        with_lock(lock, async {
            // Transactional update graphs
            let txn_ops: Vec<(Operation, Keyspace, String)> = updated_graphs
                .into_iter()
                .map(|(job_id, graph)| {
                    let value = self.encode_execution_graph(graph)?;
                    Ok((Operation::Put(value), Keyspace::ActiveJobs, job_id))
                })
                .collect::<Result<Vec<_>>>()?;
            self.state.apply_txn(txn_ops).await?;
            Ok(running_tasks_to_cancel)
        })
        .await
    }

    #[allow(dead_code)]
    #[cfg(not(test))]
    /// Launch the given task on the specified executor
    pub(crate) async fn launch_task(
        &self,
        executor: &ExecutorMetadata,
        task: TaskDescription,
        executor_manager: &ExecutorManager,
    ) -> Result<()> {
        info!("Launching task {:?} on executor {:?}", task, executor.id);
        let task_definition = self.prepare_task_definition(task)?;
        let mut client = executor_manager.get_client(&executor.id).await?;
        client
            .launch_task(protobuf::LaunchTaskParams {
                tasks: vec![task_definition],
                scheduler_id: self.scheduler_id.clone(),
            })
            .await
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Failed to connect to executor {}: {:?}",
                    executor.id, e
                ))
            })?;
        Ok(())
    }

    #[allow(dead_code)]
    #[cfg(test)]
    /// In unit tests, we do not have actual executors running, so it simplifies things to just noop.
    pub(crate) async fn launch_task(
        &self,
        _executor: &ExecutorMetadata,
        _task: TaskDescription,
        _executor_manager: &ExecutorManager,
    ) -> Result<()> {
        Ok(())
    }

    /// Retrieve the number of available tasks for the given job. The value returned
    /// is strictly a point-in-time snapshot
    pub async fn get_available_task_count(&self, job_id: &str) -> Result<usize> {
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let available_tasks = graph.read().await.available_tasks();
            Ok(available_tasks)
        } else {
            warn!("Fail to find job {} in the cache", job_id);
            Ok(0)
        }
    }

    #[allow(dead_code)]
    pub fn prepare_task_definition(
        &self,
        task: TaskDescription,
    ) -> Result<TaskDefinition> {
        debug!("Preparing task definition for {:?}", task);

        let job_id = task.partition.job_id.clone();
        let stage_id = task.partition.stage_id;

        if let Some(mut job_info) = self.active_job_cache.get_mut(&job_id) {
            let plan = if let Some(plan) = job_info.encoded_stage_plans.get(&stage_id) {
                plan.clone()
            } else {
                let mut plan_buf: Vec<u8> = vec![];
                let plan_proto = U::try_from_physical_plan(
                    task.plan,
                    self.codec.physical_extension_codec(),
                )?;
                plan_proto.try_encode(&mut plan_buf)?;

                job_info
                    .encoded_stage_plans
                    .insert(stage_id, plan_buf.clone());

                plan_buf
            };

            let output_partitioning =
                hash_partitioning_to_proto(task.output_partitioning.as_ref())?;

            let task_definition = TaskDefinition {
                task_id: task.task_id as u32,
                task_attempt_num: task.task_attempt as u32,
                job_id,
                stage_id: stage_id as u32,
                stage_attempt_num: task.stage_attempt_num as u32,
                partition_id: task.partition.partition_id as u32,
                plan,
                output_partitioning,
                session_id: task.session_id,
                launch_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                props: vec![],
            };
            Ok(task_definition)
        } else {
            Err(BallistaError::General(format!(
                "Cannot prepare task definition for job {} which is not in active cache",
                job_id
            )))
        }
    }

    #[cfg(not(test))]
    /// Launch the given tasks on the specified executor
    pub(crate) async fn launch_multi_task(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<Vec<TaskDescription>>,
        executor_manager: &ExecutorManager,
    ) -> Result<()> {
        info!("Launching multi task on executor {:?}", executor.id);
        let multi_tasks: Result<Vec<MultiTaskDefinition>> = tasks
            .into_iter()
            .map(|stage_tasks| self.prepare_multi_task_definition(stage_tasks))
            .collect();
        let multi_tasks = multi_tasks?;
        let mut client = executor_manager.get_client(&executor.id).await?;
        client
            .launch_multi_task(protobuf::LaunchMultiTaskParams {
                multi_tasks,
                scheduler_id: self.scheduler_id.clone(),
            })
            .await
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Failed to connect to executor {}: {:?}",
                    executor.id, e
                ))
            })?;
        Ok(())
    }

    #[cfg(test)]
    /// Launch the given tasks on the specified executor
    pub(crate) async fn launch_multi_task(
        &self,
        _executor: &ExecutorMetadata,
        _tasks: Vec<Vec<TaskDescription>>,
        _executor_manager: &ExecutorManager,
    ) -> Result<()> {
        Ok(())
    }

    #[allow(dead_code)]
    /// Prepare a MultiTaskDefinition with multiple tasks belonging to the same job stage
    fn prepare_multi_task_definition(
        &self,
        tasks: Vec<TaskDescription>,
    ) -> Result<MultiTaskDefinition> {
        if let Some(task) = tasks.get(0) {
            let session_id = task.session_id.clone();
            let job_id = task.partition.job_id.clone();
            let stage_id = task.partition.stage_id;
            let stage_attempt_num = task.stage_attempt_num;

            if log::max_level() >= log::Level::Debug {
                let task_ids: Vec<usize> = tasks
                    .iter()
                    .map(|task| task.partition.partition_id)
                    .collect();
                debug!("Preparing multi task definition for tasks {:?} belonging to job stage {}/{}", task_ids, job_id, stage_id);
                trace!("With task details {:?}", tasks);
            }

            if let Some(mut job_info) = self.active_job_cache.get_mut(&job_id) {
                let plan = if let Some(plan) = job_info.encoded_stage_plans.get(&stage_id)
                {
                    plan.clone()
                } else {
                    let mut plan_buf: Vec<u8> = vec![];
                    let plan_proto = U::try_from_physical_plan(
                        task.plan.clone(),
                        self.codec.physical_extension_codec(),
                    )?;
                    plan_proto.try_encode(&mut plan_buf)?;

                    job_info
                        .encoded_stage_plans
                        .insert(stage_id, plan_buf.clone());

                    plan_buf
                };
                let output_partitioning =
                    hash_partitioning_to_proto(task.output_partitioning.as_ref())?;

                let task_ids = tasks
                    .iter()
                    .map(|task| TaskId {
                        task_id: task.task_id as u32,
                        task_attempt_num: task.task_attempt as u32,
                        partition_id: task.partition.partition_id as u32,
                    })
                    .collect();

                let multi_task_definition = MultiTaskDefinition {
                    task_ids,
                    job_id,
                    stage_id: stage_id as u32,
                    stage_attempt_num: stage_attempt_num as u32,
                    plan,
                    output_partitioning,
                    session_id,
                    launch_time: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    props: vec![],
                };
                Ok(multi_task_definition)
            } else {
                Err(BallistaError::General(format!("Cannot prepare multi task definition for job {} which is not in active cache", job_id)))
            }
        } else {
            Err(BallistaError::General(
                "Cannot prepare multi task definition for an empty vec".to_string(),
            ))
        }
    }

    /// Get the `ExecutionGraph` for the given job ID from cache
    pub(crate) async fn get_active_execution_graph(
        &self,
        job_id: &str,
    ) -> Option<Arc<RwLock<ExecutionGraph>>> {
        self.active_job_cache
            .get(job_id)
            .map(|value| value.execution_graph.clone())
    }

    /// Remove the `ExecutionGraph` for the given job ID from cache
    pub(crate) async fn remove_active_execution_graph(
        &self,
        job_id: &str,
    ) -> Option<Arc<RwLock<ExecutionGraph>>> {
        self.active_job_cache
            .remove(job_id)
            .map(|value| value.1.execution_graph)
    }

    /// Get the `ExecutionGraph` for the given job ID. This will search fist in the `ActiveJobs`
    /// keyspace and then, if it doesn't find anything, search the `CompletedJobs` keyspace.
    pub(crate) async fn get_execution_graph(
        &self,
        job_id: &str,
    ) -> Result<ExecutionGraph> {
        let value = self.state.get(Keyspace::ActiveJobs, job_id).await?;

        if value.is_empty() {
            let value = self.state.get(Keyspace::CompletedJobs, job_id).await?;
            self.decode_execution_graph(value).await
        } else {
            self.decode_execution_graph(value).await
        }
    }

    async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        let value = self.state.get(Keyspace::Sessions, session_id).await?;

        let settings: protobuf::SessionSettings = decode_protobuf(&value)?;

        let mut config_builder = BallistaConfig::builder();
        for kv_pair in &settings.configs {
            config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
        }
        let config = config_builder.build()?;

        create_datafusion_context(&config, self.session_builder.as_ref())
    }

    async fn decode_execution_graph(&self, value: Vec<u8>) -> Result<ExecutionGraph> {
        let proto: protobuf::ExecutionGraph = decode_protobuf(&value)?;

        let session_id = &proto.session_id;

        let session_ctx = self.get_session(session_id).await?;

        ExecutionGraph::decode_execution_graph(proto, &self.codec, &session_ctx).await
    }

    fn encode_execution_graph(&self, graph: ExecutionGraph) -> Result<Vec<u8>> {
        let proto = ExecutionGraph::encode_execution_graph(graph, &self.codec)?;

        encode_protobuf(&proto)
    }

    /// Generate a new random Job ID
    pub fn generate_job_id(&self) -> String {
        let mut rng = thread_rng();
        std::iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(7)
            .collect()
    }

    async fn clean_up_job_data(
        state: Arc<dyn StateBackendClient>,
        active_job_cache: ActiveJobCache,
        failed: bool,
        job_id: String,
    ) -> Result<()> {
        active_job_cache.remove(&job_id);
        let keyspace = if failed {
            Keyspace::FailedJobs
        } else {
            Keyspace::CompletedJobs
        };

        let lock = state.lock(keyspace.clone(), "").await?;
        with_lock(lock, state.delete(keyspace, &job_id)).await?;

        Ok(())
    }
}

pub struct JobOverview {
    pub job_id: String,
    pub job_name: String,
    pub status: JobStatus,
    pub start_time: u64,
    pub end_time: u64,
    pub num_stages: usize,
    pub completed_stages: usize,
}
