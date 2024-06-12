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

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{accept, ExecutionPlan, ExecutionPlanVisitor};
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use log::{error, info, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::{ShuffleWriterExec, UnresolvedShuffleExec};
use ballista_core::serde::protobuf::failed_task::FailedReason;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{
    self, execution_graph_stage::StageType, FailedTask, JobStatus, ResultLost,
    RunningJob, SuccessfulJob, TaskStatus,
};
use ballista_core::serde::protobuf::{job_status, FailedJob, ShuffleWritePartition};
use ballista_core::serde::protobuf::{task_status, RunningTask};
use ballista_core::serde::scheduler::{
    ExecutorMetadata, PartitionId, PartitionLocation, PartitionStats,
};
use ballista_core::serde::BallistaCodec;
use datafusion_proto::physical_plan::AsExecutionPlan;

use crate::display::print_stage_metrics;
use crate::planner::DistributedPlanner;
use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::timestamp_millis;
use crate::state::execution_graph::execution_stage::RunningStage;
pub(crate) use crate::state::execution_graph::execution_stage::{
    ExecutionStage, FailedStage, ResolvedStage, StageOutput, SuccessfulStage, TaskInfo,
    UnresolvedStage,
};
use crate::state::task_manager::UpdatedStages;

mod execution_stage;

/// Represents the DAG for a distributed query plan.
///
/// A distributed query plan consists of a set of stages which must be executed sequentially.
///
/// Each stage consists of a set of partitions which can be executed in parallel, where each partition
/// represents a `Task`, which is the basic unit of scheduling in Ballista.
///
/// As an example, consider a SQL query which performs a simple aggregation:
///
/// `SELECT id, SUM(gmv) FROM some_table GROUP BY id`
///
/// This will produce a DataFusion execution plan that looks something like
///
///
///   CoalesceBatchesExec: target_batch_size=4096
///     RepartitionExec: partitioning=Hash([Column { name: "id", index: 0 }], 4)
///       AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[SUM(some_table.gmv)]
///         TableScan: some_table
///
/// The Ballista `DistributedPlanner` will turn this into a distributed plan by creating a shuffle
/// boundary (called a "Stage") whenever the underlying plan needs to perform a repartition.
/// In this case we end up with a distributed plan with two stages:
///
///
/// ExecutionGraph[job_id=job, session_id=session, available_tasks=1, complete=false]
/// =========UnResolvedStage[id=2, children=1]=========
/// Inputs{1: StageOutput { partition_locations: {}, complete: false }}
/// ShuffleWriterExec: None
///   AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[SUM(?table?.gmv)]
///     CoalesceBatchesExec: target_batch_size=4096
///       UnresolvedShuffleExec
/// =========ResolvedStage[id=1, partitions=1]=========
/// ShuffleWriterExec: Some(Hash([Column { name: "id", index: 0 }], 4))
///   AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[SUM(?table?.gmv)]
///     TableScan: some_table
///
///
/// The DAG structure of this `ExecutionGraph` is encoded in the stages. Each stage's `input` field
/// will indicate which stages it depends on, and each stage's `output_links` will indicate which
/// stage it needs to publish its output to.
///
/// If a stage has `output_links` is empty then it is the final stage in this query, and it should
/// publish its outputs to the `ExecutionGraph`s `output_locations` representing the final query results.
#[derive(Clone)]
pub struct ExecutionGraph {
    /// Curator scheduler name. Can be `None` is `ExecutionGraph` is not currently curated by any scheduler
    scheduler_id: Option<String>,
    /// ID for this job
    job_id: String,
    /// Job name, can be empty string
    job_name: String,
    /// Session ID for this job
    session_id: String,
    /// Status of this job
    status: JobStatus,
    /// Timestamp of when this job was submitted
    queued_at: u64,
    /// Job start time
    start_time: u64,
    /// Job end time
    end_time: u64,
    /// Map from Stage ID -> ExecutionStage
    stages: HashMap<usize, ExecutionStage>,
    /// Total number fo output partitions
    output_partitions: usize,
    /// Locations of this `ExecutionGraph` final output locations
    output_locations: Vec<PartitionLocation>,
    /// Task ID generator, generate unique TID in the execution graph
    task_id_gen: usize,
    /// Failed stage attempts, record the failed stage attempts to limit the retry times.
    /// Map from Stage ID -> Set<Stage_ATTPMPT_NUM>
    failed_stage_attempts: HashMap<usize, HashSet<usize>>,
}

#[derive(Clone, Debug)]
pub struct RunningTaskInfo {
    pub task_id: usize,
    pub job_id: String,
    pub stage_id: usize,
    pub partition_id: usize,
    pub executor_id: String,
}

impl ExecutionGraph {
    pub fn new(
        scheduler_id: &str,
        job_id: &str,
        job_name: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
        queued_at: u64,
    ) -> Result<Self> {
        let mut planner = DistributedPlanner::new();

        let output_partitions = plan.output_partitioning().partition_count();

        let shuffle_stages = planner.plan_query_stages(job_id, plan)?;

        let builder = ExecutionStageBuilder::new();
        let stages = builder.build(shuffle_stages)?;

        let started_at = timestamp_millis();

        Ok(Self {
            scheduler_id: Some(scheduler_id.to_string()),
            job_id: job_id.to_string(),
            job_name: job_name.to_string(),
            session_id: session_id.to_string(),
            status: JobStatus {
                job_id: job_id.to_string(),
                job_name: job_name.to_string(),
                status: Some(Status::Running(RunningJob {
                    queued_at,
                    started_at,
                    scheduler: scheduler_id.to_string(),
                })),
            },
            queued_at,
            start_time: started_at,
            end_time: 0,
            stages,
            output_partitions,
            output_locations: vec![],
            task_id_gen: 0,
            failed_stage_attempts: HashMap::new(),
        })
    }

    pub fn job_id(&self) -> &str {
        self.job_id.as_str()
    }

    pub fn job_name(&self) -> &str {
        self.job_name.as_str()
    }

    pub fn session_id(&self) -> &str {
        self.session_id.as_str()
    }

    pub fn status(&self) -> &JobStatus {
        &self.status
    }

    pub fn start_time(&self) -> u64 {
        self.start_time
    }

    pub fn end_time(&self) -> u64 {
        self.end_time
    }

    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    pub fn next_task_id(&mut self) -> usize {
        let new_tid = self.task_id_gen;
        self.task_id_gen += 1;
        new_tid
    }

    pub(crate) fn stages(&self) -> &HashMap<usize, ExecutionStage> {
        &self.stages
    }

    /// An ExecutionGraph is successful if all its stages are successful
    pub fn is_successful(&self) -> bool {
        self.stages
            .values()
            .all(|s| matches!(s, ExecutionStage::Successful(_)))
    }

    pub fn is_complete(&self) -> bool {
        self.stages
            .values()
            .all(|s| matches!(s, ExecutionStage::Successful(_)))
    }

    /// Revive the execution graph by converting the resolved stages to running stages
    /// If any stages are converted, return true; else false.
    pub fn revive(&mut self) -> bool {
        let running_stages = self
            .stages
            .values()
            .filter_map(|stage| {
                if let ExecutionStage::Resolved(resolved_stage) = stage {
                    Some(resolved_stage.to_running())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if running_stages.is_empty() {
            false
        } else {
            for running_stage in running_stages {
                self.stages.insert(
                    running_stage.stage_id,
                    ExecutionStage::Running(running_stage),
                );
            }
            true
        }
    }

    /// Update task statuses and task metrics in the graph.
    /// This will also push shuffle partitions to their respective shuffle read stages.
    pub fn update_task_status(
        &mut self,
        executor: &ExecutorMetadata,
        task_statuses: Vec<TaskStatus>,
        max_task_failures: usize,
        max_stage_failures: usize,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let job_id = self.job_id().to_owned();
        // First of all, classify the statuses by stages
        let mut job_task_statuses: HashMap<usize, Vec<TaskStatus>> = HashMap::new();
        for task_status in task_statuses {
            let stage_id = task_status.stage_id as usize;
            let stage_task_statuses = job_task_statuses.entry(stage_id).or_default();
            stage_task_statuses.push(task_status);
        }

        // Revive before updating due to some updates not saved
        // It will be refined later
        self.revive();

        let current_running_stages: HashSet<usize> =
            HashSet::from_iter(self.running_stages());

        // Copy the failed stage attempts from self
        let mut failed_stage_attempts: HashMap<usize, HashSet<usize>> = HashMap::new();
        for (stage_id, attempts) in self.failed_stage_attempts.iter() {
            failed_stage_attempts
                .insert(*stage_id, HashSet::from_iter(attempts.iter().copied()));
        }

        let mut resolved_stages = HashSet::new();
        let mut successful_stages = HashSet::new();
        let mut failed_stages = HashMap::new();
        let mut rollback_running_stages = HashMap::new();
        let mut resubmit_successful_stages: HashMap<usize, HashSet<usize>> =
            HashMap::new();
        let mut reset_running_stages: HashMap<usize, HashSet<usize>> = HashMap::new();

        for (stage_id, stage_task_statuses) in job_task_statuses {
            if let Some(stage) = self.stages.get_mut(&stage_id) {
                if let ExecutionStage::Running(running_stage) = stage {
                    let mut locations = vec![];
                    for task_status in stage_task_statuses.into_iter() {
                        let task_stage_attempt_num =
                            task_status.stage_attempt_num as usize;
                        if task_stage_attempt_num < running_stage.stage_attempt_num {
                            warn!("Ignore TaskStatus update with TID {} as it's from Stage {}.{} and there is a more recent stage attempt {}.{} running",
                                    task_status.task_id, stage_id, task_stage_attempt_num, stage_id, running_stage.stage_attempt_num);
                            continue;
                        }
                        let partition_id = task_status.clone().partition_id as usize;
                        let task_identity = format!(
                            "TID {} {}/{}.{}/{}",
                            task_status.task_id,
                            job_id,
                            stage_id,
                            task_stage_attempt_num,
                            partition_id
                        );
                        let operator_metrics = task_status.metrics.clone();

                        if !running_stage
                            .update_task_info(partition_id, task_status.clone())
                        {
                            continue;
                        }

                        if let Some(task_status::Status::Failed(failed_task)) =
                            task_status.status
                        {
                            let failed_reason = failed_task.failed_reason;

                            match failed_reason {
                                Some(FailedReason::FetchPartitionError(
                                    fetch_partition_error,
                                )) => {
                                    let failed_attempts = failed_stage_attempts
                                        .entry(stage_id)
                                        .or_default();
                                    failed_attempts.insert(task_stage_attempt_num);
                                    if failed_attempts.len() < max_stage_failures {
                                        let map_stage_id =
                                            fetch_partition_error.map_stage_id as usize;
                                        let map_partition_id = fetch_partition_error
                                            .map_partition_id
                                            as usize;
                                        let executor_id =
                                            fetch_partition_error.executor_id;

                                        if !failed_stages.is_empty() {
                                            let error_msg = format!(
                                                "Stages was marked failed, ignore FetchPartitionError from task {task_identity}");
                                            warn!("{}", error_msg);
                                        } else {
                                            // There are different removal strategies here.
                                            // We can choose just remove the map_partition_id in the FetchPartitionError, when resubmit the input stage, there are less tasks
                                            // need to rerun, but this might miss many more bad input partitions, lead to more stage level retries in following.
                                            // Here we choose remove all the bad input partitions which match the same executor id in this single input stage.
                                            // There are other more aggressive approaches, like considering the executor is lost and check all the running stages in this graph.
                                            // Or count the fetch failure number on executor and mark the executor lost globally.
                                            let removed_map_partitions = running_stage
                                                .remove_input_partitions(
                                                    map_stage_id,
                                                    map_partition_id,
                                                    &executor_id,
                                                )?;

                                            let failure_reasons = rollback_running_stages
                                                .entry(stage_id)
                                                .or_insert_with(HashSet::new);
                                            failure_reasons.insert(executor_id);

                                            let missing_inputs =
                                                resubmit_successful_stages
                                                    .entry(map_stage_id)
                                                    .or_default();
                                            missing_inputs.extend(removed_map_partitions);
                                            warn!("Need to resubmit the current running Stage {} and its map Stage {} due to FetchPartitionError from task {}",
                                                    stage_id, map_stage_id, task_identity)
                                        }
                                    } else {
                                        let error_msg = format!(
                                            "Stage {} has failed {} times, \
                                            most recent failure reason: {:?}",
                                            stage_id,
                                            max_stage_failures,
                                            failed_task.error
                                        );
                                        error!("{}", error_msg);
                                        failed_stages.insert(stage_id, error_msg);
                                    }
                                }
                                Some(FailedReason::ExecutionError(_)) => {
                                    failed_stages.insert(stage_id, failed_task.error);
                                }
                                Some(_) => {
                                    if failed_task.retryable
                                        && failed_task.count_to_failures
                                    {
                                        if running_stage.task_failure_number(partition_id)
                                            < max_task_failures
                                        {
                                            // TODO add new struct to track all the failed task infos
                                            // The failure TaskInfo is ignored and set to None here
                                            running_stage.reset_task_info(partition_id);
                                        } else {
                                            let error_msg = format!(
                                                "Task {} in Stage {} failed {} times, fail the stage, most recent failure reason: {:?}",
                                                partition_id, stage_id, max_task_failures, failed_task.error
                                            );
                                            error!("{}", error_msg);
                                            failed_stages.insert(stage_id, error_msg);
                                        }
                                    } else if failed_task.retryable {
                                        // TODO add new struct to track all the failed task infos
                                        // The failure TaskInfo is ignored and set to None here
                                        running_stage.reset_task_info(partition_id);
                                    }
                                }
                                None => {
                                    let error_msg = format!(
                                        "Task {partition_id} in Stage {stage_id} failed with unknown failure reasons, fail the stage");
                                    error!("{}", error_msg);
                                    failed_stages.insert(stage_id, error_msg);
                                }
                            }
                        } else if let Some(task_status::Status::Successful(
                            successful_task,
                        )) = task_status.status
                        {
                            // update task metrics for successful task
                            running_stage
                                .update_task_metrics(partition_id, operator_metrics)?;

                            locations.append(&mut partition_to_location(
                                &job_id,
                                partition_id,
                                stage_id,
                                executor,
                                successful_task.partitions,
                            ));
                        } else {
                            warn!(
                                "The task {}'s status is invalid for updating",
                                task_identity
                            );
                        }
                    }

                    let is_final_successful = running_stage.is_successful()
                        && !reset_running_stages.contains_key(&stage_id);
                    if is_final_successful {
                        successful_stages.insert(stage_id);
                        // if this stage is final successful, we want to combine the stage metrics to plan's metric set and print out the plan
                        if let Some(stage_metrics) = running_stage.stage_metrics.as_ref()
                        {
                            print_stage_metrics(
                                &job_id,
                                stage_id,
                                running_stage.plan.as_ref(),
                                stage_metrics,
                            );
                        }
                    }

                    let output_links = running_stage.output_links.clone();
                    resolved_stages.extend(
                        &mut self
                            .update_stage_output_links(
                                stage_id,
                                is_final_successful,
                                locations,
                                output_links,
                            )?
                            .into_iter(),
                    );
                } else if let ExecutionStage::UnResolved(unsolved_stage) = stage {
                    for task_status in stage_task_statuses.into_iter() {
                        let task_stage_attempt_num =
                            task_status.stage_attempt_num as usize;
                        let partition_id = task_status.clone().partition_id as usize;
                        let task_identity = format!(
                            "TID {} {}/{}.{}/{}",
                            task_status.task_id,
                            job_id,
                            stage_id,
                            task_stage_attempt_num,
                            partition_id
                        );
                        let mut should_ignore = true;
                        // handle delayed failed tasks if the stage's next attempt is still in UnResolved status.
                        if let Some(task_status::Status::Failed(failed_task)) =
                            task_status.status
                        {
                            if unsolved_stage.stage_attempt_num - task_stage_attempt_num
                                == 1
                            {
                                let failed_reason = failed_task.failed_reason;
                                match failed_reason {
                                    Some(FailedReason::ExecutionError(_)) => {
                                        should_ignore = false;
                                        failed_stages.insert(stage_id, failed_task.error);
                                    }
                                    Some(FailedReason::FetchPartitionError(
                                        fetch_partition_error,
                                    )) if failed_stages.is_empty()
                                        && current_running_stages.contains(
                                            &(fetch_partition_error.map_stage_id
                                                as usize),
                                        )
                                        && !unsolved_stage
                                            .last_attempt_failure_reasons
                                            .contains(
                                                &fetch_partition_error.executor_id,
                                            ) =>
                                    {
                                        should_ignore = false;
                                        unsolved_stage
                                            .last_attempt_failure_reasons
                                            .insert(
                                                fetch_partition_error.executor_id.clone(),
                                            );
                                        let map_stage_id =
                                            fetch_partition_error.map_stage_id as usize;
                                        let map_partition_id = fetch_partition_error
                                            .map_partition_id
                                            as usize;
                                        let executor_id =
                                            fetch_partition_error.executor_id;
                                        let removed_map_partitions = unsolved_stage
                                            .remove_input_partitions(
                                                map_stage_id,
                                                map_partition_id,
                                                &executor_id,
                                            )?;

                                        let missing_inputs = reset_running_stages
                                            .entry(map_stage_id)
                                            .or_default();
                                        missing_inputs.extend(removed_map_partitions);
                                        warn!("Need to reset the current running Stage {} due to late come FetchPartitionError from its parent stage {} of task {}",
                                                    map_stage_id, stage_id, task_identity);

                                        // If the previous other task updates had already mark the map stage success, need to remove it.
                                        if successful_stages.contains(&map_stage_id) {
                                            successful_stages.remove(&map_stage_id);
                                        }
                                        if resolved_stages.contains(&stage_id) {
                                            resolved_stages.remove(&stage_id);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        if should_ignore {
                            warn!("Ignore TaskStatus update of task with TID {} as the Stage {}/{} is in UnResolved status", task_identity, job_id, stage_id);
                        }
                    }
                } else {
                    warn!(
                        "Stage {}/{} is not in running when updating the status of tasks {:?}",
                        job_id,
                        stage_id,
                        stage_task_statuses.into_iter().map(|task_status| task_status.partition_id).collect::<Vec<_>>(),
                    );
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {stage_id} for job {job_id}"
                )));
            }
        }

        // Update failed stage attempts back to self
        for (stage_id, attempts) in failed_stage_attempts.iter() {
            self.failed_stage_attempts
                .insert(*stage_id, HashSet::from_iter(attempts.iter().copied()));
        }

        for (stage_id, missing_parts) in &resubmit_successful_stages {
            if let Some(stage) = self.stages.get_mut(stage_id) {
                if let ExecutionStage::Successful(success_stage) = stage {
                    for partition in missing_parts {
                        if *partition > success_stage.partitions {
                            return Err(BallistaError::Internal(format!(
                                "Invalid partition ID {} in map stage {}",
                                *partition, stage_id
                            )));
                        }
                        let task_info = &mut success_stage.task_infos[*partition];
                        // Update the task info to failed
                        task_info.task_status = task_status::Status::Failed(FailedTask {
                            error: "FetchPartitionError in parent stage".to_owned(),
                            retryable: true,
                            count_to_failures: false,
                            failed_reason: Some(FailedReason::ResultLost(ResultLost {})),
                        });
                    }
                } else {
                    warn!(
                        "Stage {}/{} is not in Successful state when try to resubmit this stage. ",
                        job_id,
                        stage_id);
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {stage_id} for job {job_id}"
                )));
            }
        }

        for (stage_id, missing_parts) in &reset_running_stages {
            if let Some(stage) = self.stages.get_mut(stage_id) {
                if let ExecutionStage::Running(running_stage) = stage {
                    for partition in missing_parts {
                        if *partition > running_stage.partitions {
                            return Err(BallistaError::Internal(format!(
                                "Invalid partition ID {} in map stage {}",
                                *partition, stage_id
                            )));
                        }
                        running_stage.reset_task_info(*partition);
                    }
                } else {
                    warn!(
                        "Stage {}/{} is not in Running state when try to reset the running task. ",
                        job_id,
                        stage_id);
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {stage_id} for job {job_id}"
                )));
            }
        }

        self.processing_stages_update(UpdatedStages {
            resolved_stages,
            successful_stages,
            failed_stages,
            rollback_running_stages,
            resubmit_successful_stages: resubmit_successful_stages
                .keys()
                .cloned()
                .collect(),
        })
    }

    /// Processing stage status update after task status changing
    fn processing_stages_update(
        &mut self,
        updated_stages: UpdatedStages,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let job_id = self.job_id().to_owned();
        let mut has_resolved = false;
        let mut job_err_msg = "".to_owned();

        for stage_id in updated_stages.resolved_stages {
            self.resolve_stage(stage_id)?;
            has_resolved = true;
        }

        for stage_id in updated_stages.successful_stages {
            self.succeed_stage(stage_id);
        }

        // Fail the stage and also abort the job
        for (stage_id, err_msg) in &updated_stages.failed_stages {
            job_err_msg =
                format!("Job failed due to stage {stage_id} failed: {err_msg}\n");
        }

        let mut events = vec![];
        // Only handle the rollback logic when there are no failed stages
        if updated_stages.failed_stages.is_empty() {
            let mut running_tasks_to_cancel = vec![];
            for (stage_id, failure_reasons) in updated_stages.rollback_running_stages {
                let tasks = self.rollback_running_stage(stage_id, failure_reasons)?;
                running_tasks_to_cancel.extend(tasks);
            }

            for stage_id in updated_stages.resubmit_successful_stages {
                self.rerun_successful_stage(stage_id);
            }

            if !running_tasks_to_cancel.is_empty() {
                events.push(QueryStageSchedulerEvent::CancelTasks(
                    running_tasks_to_cancel,
                ));
            }
        }

        if !updated_stages.failed_stages.is_empty() {
            info!("Job {} is failed", job_id);
            self.fail_job(job_err_msg.clone());
            events.push(QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                fail_message: job_err_msg,
                queued_at: self.queued_at,
                failed_at: timestamp_millis(),
            });
        } else if self.is_successful() {
            // If this ExecutionGraph is successful, finish it
            info!("Job {} is success, finalizing output partitions", job_id);
            self.succeed_job()?;
            events.push(QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at: self.queued_at,
                completed_at: timestamp_millis(),
            });
        } else if has_resolved {
            events.push(QueryStageSchedulerEvent::JobUpdated(job_id))
        }
        Ok(events)
    }

    /// Return a Vec of resolvable stage ids
    fn update_stage_output_links(
        &mut self,
        stage_id: usize,
        is_completed: bool,
        locations: Vec<PartitionLocation>,
        output_links: Vec<usize>,
    ) -> Result<Vec<usize>> {
        let mut resolved_stages = vec![];
        let job_id = &self.job_id;
        if output_links.is_empty() {
            // If `output_links` is empty, then this is a final stage
            self.output_locations.extend(locations);
        } else {
            for link in output_links.iter() {
                // If this is an intermediate stage, we need to push its `PartitionLocation`s to the parent stage
                if let Some(linked_stage) = self.stages.get_mut(link) {
                    if let ExecutionStage::UnResolved(linked_unresolved_stage) =
                        linked_stage
                    {
                        linked_unresolved_stage
                            .add_input_partitions(stage_id, locations.clone())?;

                        // If all tasks for this stage are complete, mark the input complete in the parent stage
                        if is_completed {
                            linked_unresolved_stage.complete_input(stage_id);
                        }

                        // If all input partitions are ready, we can resolve any UnresolvedShuffleExec in the parent stage plan
                        if linked_unresolved_stage.resolvable() {
                            resolved_stages.push(linked_unresolved_stage.stage_id);
                        }
                    } else {
                        return Err(BallistaError::Internal(format!(
                            "Error updating job {job_id}: The stage {link} as the output link of stage {stage_id}  should be unresolved"
                        )));
                    }
                } else {
                    return Err(BallistaError::Internal(format!(
                        "Error updating job {job_id}: Invalid output link {stage_id} for stage {link}"
                    )));
                }
            }
        }
        Ok(resolved_stages)
    }

    /// Return all the currently running stage ids
    pub fn running_stages(&self) -> Vec<usize> {
        self.stages
            .iter()
            .filter_map(|(stage_id, stage)| {
                if let ExecutionStage::Running(_running) = stage {
                    Some(*stage_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    /// Return all currently running tasks along with the executor ID on which they are assigned
    pub fn running_tasks(&self) -> Vec<RunningTaskInfo> {
        self.stages
            .iter()
            .flat_map(|(_, stage)| {
                if let ExecutionStage::Running(stage) = stage {
                    stage
                        .running_tasks()
                        .into_iter()
                        .map(|(task_id, stage_id, partition_id, executor_id)| {
                            RunningTaskInfo {
                                task_id,
                                job_id: self.job_id.clone(),
                                stage_id,
                                partition_id,
                                executor_id,
                            }
                        })
                        .collect::<Vec<RunningTaskInfo>>()
                } else {
                    vec![]
                }
            })
            .collect::<Vec<RunningTaskInfo>>()
    }

    /// Total number of tasks in this plan that are ready for scheduling
    pub fn available_tasks(&self) -> usize {
        self.stages
            .values()
            .map(|stage| {
                if let ExecutionStage::Running(stage) = stage {
                    stage.available_tasks()
                } else {
                    0
                }
            })
            .sum()
    }

    /// Get next task that can be assigned to the given executor.
    /// This method should only be called when the resulting task is immediately
    /// being launched as the status will be set to Running and it will not be
    /// available to the scheduler.
    /// If the task is not launched the status must be reset to allow the task to
    /// be scheduled elsewhere.
    pub fn pop_next_task(
        &mut self,
        executor_id: &str,
    ) -> Result<Option<TaskDescription>> {
        if matches!(
            self.status,
            JobStatus {
                status: Some(job_status::Status::Failed(_)),
                ..
            }
        ) {
            warn!("Call pop_next_task on failed Job");
            return Ok(None);
        }

        let job_id = self.job_id.clone();
        let session_id = self.session_id.clone();

        let find_candidate = self.stages.iter().any(|(_stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                stage.available_tasks() > 0
            } else {
                false
            }
        });
        let next_task_id = if find_candidate {
            Some(self.next_task_id())
        } else {
            None
        };

        let mut next_task = self.stages.iter_mut().find(|(_stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                stage.available_tasks() > 0
            } else {
                false
            }
        }).map(|(stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                let (partition_id, _) = stage
                    .task_infos
                    .iter()
                    .enumerate()
                    .find(|(_partition, info)| info.is_none())
                    .ok_or_else(|| {
                        BallistaError::Internal(format!("Error getting next task for job {job_id}: Stage {stage_id} is ready but has no pending tasks"))
                    })?;

                let partition = PartitionId {
                    job_id,
                    stage_id: *stage_id,
                    partition_id,
                };

                let task_id = next_task_id.unwrap();
                let task_attempt = stage.task_failure_numbers[partition_id];
                let task_info = TaskInfo {
                    task_id,
                    scheduled_time: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis(),
                    // Those times will be updated when the task finish
                    launch_time: 0,
                    start_exec_time: 0,
                    end_exec_time: 0,
                    finish_time: 0,
                    task_status: task_status::Status::Running(RunningTask {
                        executor_id: executor_id.to_owned()
                    }),
                };

                // Set the task info to Running for new task
                stage.task_infos[partition_id] = Some(task_info);

                Ok(TaskDescription {
                    session_id,
                    partition,
                    stage_attempt_num: stage.stage_attempt_num,
                    task_id,
                    task_attempt,
                    data_cache: false,
                    plan: stage.plan.clone(),
                })
            } else {
                Err(BallistaError::General(format!("Stage {stage_id} is not a running stage")))
            }
        }).transpose()?;

        // If no available tasks found in the running stage,
        // try to find a resolved stage and convert it to the running stage
        if next_task.is_none() {
            if self.revive() {
                next_task = self.pop_next_task(executor_id)?;
            } else {
                next_task = None;
            }
        }

        Ok(next_task)
    }

    pub(crate) fn fetch_running_stage(
        &mut self,
        black_list: &[usize],
    ) -> Option<(&mut RunningStage, &mut usize)> {
        if matches!(
            self.status,
            JobStatus {
                status: Some(job_status::Status::Failed(_)),
                ..
            }
        ) {
            warn!("Call fetch_runnable_stage on failed Job");
            return None;
        }

        let running_stage_id = self.get_running_stage_id(black_list);
        if let Some(running_stage_id) = running_stage_id {
            if let Some(ExecutionStage::Running(running_stage)) =
                self.stages.get_mut(&running_stage_id)
            {
                Some((running_stage, &mut self.task_id_gen))
            } else {
                warn!("Fail to find running stage with id {running_stage_id}");
                None
            }
        } else {
            None
        }
    }

    fn get_running_stage_id(&mut self, black_list: &[usize]) -> Option<usize> {
        let mut running_stage_id = self.stages.iter().find_map(|(stage_id, stage)| {
            if black_list.contains(stage_id) {
                None
            } else if let ExecutionStage::Running(stage) = stage {
                if stage.available_tasks() > 0 {
                    Some(*stage_id)
                } else {
                    None
                }
            } else {
                None
            }
        });

        // If no available tasks found in the running stage,
        // try to find a resolved stage and convert it to the running stage
        if running_stage_id.is_none() {
            if self.revive() {
                running_stage_id = self.get_running_stage_id(black_list);
            } else {
                running_stage_id = None;
            }
        }

        running_stage_id
    }

    pub fn update_status(&mut self, status: JobStatus) {
        self.status = status;
    }

    pub fn output_locations(&self) -> Vec<PartitionLocation> {
        self.output_locations.clone()
    }

    /// Reset running and successful stages on a given executor
    /// This will first check the unresolved/resolved/running stages and reset the running tasks and successful tasks.
    /// Then it will check the successful stage and whether there are running parent stages need to read shuffle from it.
    /// If yes, reset the successful tasks and roll back the resolved shuffle recursively.
    ///
    /// Returns the reset stage ids and running tasks should be killed
    pub fn reset_stages_on_lost_executor(
        &mut self,
        executor_id: &str,
    ) -> Result<(HashSet<usize>, Vec<RunningTaskInfo>)> {
        let mut reset = HashSet::new();
        let mut tasks_to_cancel = vec![];
        loop {
            let reset_stage = self.reset_stages_internal(executor_id)?;
            if !reset_stage.0.is_empty() {
                reset.extend(reset_stage.0.iter());
                tasks_to_cancel.extend(reset_stage.1)
            } else {
                return Ok((reset, tasks_to_cancel));
            }
        }
    }

    fn reset_stages_internal(
        &mut self,
        executor_id: &str,
    ) -> Result<(HashSet<usize>, Vec<RunningTaskInfo>)> {
        let job_id = self.job_id.clone();
        // collect the input stages that need to resubmit
        let mut resubmit_inputs: HashSet<usize> = HashSet::new();

        let mut reset_running_stage = HashSet::new();
        let mut rollback_resolved_stages = HashSet::new();
        let mut rollback_running_stages = HashSet::new();
        let mut resubmit_successful_stages = HashSet::new();

        let mut empty_inputs: HashMap<usize, StageOutput> = HashMap::new();
        // check the unresolved, resolved and running stages
        self.stages
            .iter_mut()
            .for_each(|(stage_id, stage)| {
                let stage_inputs = match stage {
                    ExecutionStage::UnResolved(stage) => {
                        &mut stage.inputs
                    }
                    ExecutionStage::Resolved(stage) => {
                        &mut stage.inputs
                    }
                    ExecutionStage::Running(stage) => {
                        let reset = stage.reset_tasks(executor_id);
                        if reset > 0 {
                            warn!(
                        "Reset {} tasks for running job/stage {}/{} on lost Executor {}",
                        reset, job_id, stage_id, executor_id
                        );
                            reset_running_stage.insert(*stage_id);
                        }
                        &mut stage.inputs
                    }
                    _ => &mut empty_inputs
                };

                // For each stage input, check whether there are input locations match that executor
                // and calculate the resubmit input stages if the input stages are successful.
                let mut rollback_stage = false;
                stage_inputs.iter_mut().for_each(|(input_stage_id, stage_output)| {
                    let mut match_found = false;
                    stage_output.partition_locations.iter_mut().for_each(
                        |(_partition, locs)| {
                            let before_len = locs.len();
                            locs.retain(|loc| loc.executor_meta.id != executor_id);
                            if locs.len() < before_len {
                                match_found = true;
                            }
                        },
                    );
                    if match_found {
                        stage_output.complete = false;
                        rollback_stage = true;
                        resubmit_inputs.insert(*input_stage_id);
                    }
                });

                if rollback_stage {
                    match stage {
                        ExecutionStage::Resolved(_) => {
                            rollback_resolved_stages.insert(*stage_id);
                            warn!(
                            "Roll back resolved job/stage {}/{} and change ShuffleReaderExec back to UnresolvedShuffleExec",
                            job_id, stage_id);
                        }
                        ExecutionStage::Running(_) => {
                            rollback_running_stages.insert(*stage_id);
                            warn!(
                            "Roll back running job/stage {}/{} and change ShuffleReaderExec back to UnresolvedShuffleExec",
                            job_id, stage_id);
                        }
                        _ => {}
                    }
                }
            });

        // check and reset the successful stages
        if !resubmit_inputs.is_empty() {
            self.stages
                .iter_mut()
                .filter(|(stage_id, _stage)| resubmit_inputs.contains(stage_id))
                .filter_map(|(_stage_id, stage)| {
                    if let ExecutionStage::Successful(success) = stage {
                        Some(success)
                    } else {
                        None
                    }
                })
                .for_each(|stage| {
                    let reset = stage.reset_tasks(executor_id);
                    if reset > 0 {
                        resubmit_successful_stages.insert(stage.stage_id);
                        warn!(
                            "Reset {} tasks for successful job/stage {}/{} on lost Executor {}",
                            reset, job_id, stage.stage_id, executor_id
                        )
                    }
                });
        }

        for stage_id in rollback_resolved_stages.iter() {
            self.rollback_resolved_stage(*stage_id)?;
        }

        let mut all_running_tasks = vec![];
        for stage_id in rollback_running_stages.iter() {
            let tasks = self.rollback_running_stage(
                *stage_id,
                HashSet::from([executor_id.to_owned()]),
            )?;
            all_running_tasks.extend(tasks);
        }

        for stage_id in resubmit_successful_stages.iter() {
            self.rerun_successful_stage(*stage_id);
        }

        let mut reset_stage = HashSet::new();
        reset_stage.extend(reset_running_stage);
        reset_stage.extend(rollback_resolved_stages);
        reset_stage.extend(rollback_running_stages);
        reset_stage.extend(resubmit_successful_stages);
        Ok((reset_stage, all_running_tasks))
    }

    /// Convert unresolved stage to be resolved
    pub fn resolve_stage(&mut self, stage_id: usize) -> Result<bool> {
        if let Some(ExecutionStage::UnResolved(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Resolved(stage.to_resolved()?));
            Ok(true)
        } else {
            warn!(
                "Fail to find a unresolved stage {}/{} to resolve",
                self.job_id(),
                stage_id
            );
            Ok(false)
        }
    }

    /// Convert running stage to be successful
    pub fn succeed_stage(&mut self, stage_id: usize) -> bool {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Successful(stage.to_successful()));
            self.clear_stage_failure(stage_id);
            true
        } else {
            warn!(
                "Fail to find a running stage {}/{} to make it success",
                self.job_id(),
                stage_id
            );
            false
        }
    }

    /// Convert running stage to be failed
    pub fn fail_stage(&mut self, stage_id: usize, err_msg: String) -> bool {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Failed(stage.to_failed(err_msg)));
            true
        } else {
            info!(
                "Fail to find a running stage {}/{} to fail",
                self.job_id(),
                stage_id
            );
            false
        }
    }

    /// Convert running stage to be unresolved,
    /// Returns a Vec of RunningTaskInfo for running tasks in this stage.
    pub fn rollback_running_stage(
        &mut self,
        stage_id: usize,
        failure_reasons: HashSet<String>,
    ) -> Result<Vec<RunningTaskInfo>> {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            let running_tasks = stage
                .running_tasks()
                .into_iter()
                .map(
                    |(task_id, stage_id, partition_id, executor_id)| RunningTaskInfo {
                        task_id,
                        job_id: self.job_id.clone(),
                        stage_id,
                        partition_id,
                        executor_id,
                    },
                )
                .collect();
            self.stages.insert(
                stage_id,
                ExecutionStage::UnResolved(stage.to_unresolved(failure_reasons)?),
            );
            Ok(running_tasks)
        } else {
            warn!(
                "Fail to find a running stage {}/{} to rollback",
                self.job_id(),
                stage_id
            );
            Ok(vec![])
        }
    }

    /// Convert resolved stage to be unresolved
    pub fn rollback_resolved_stage(&mut self, stage_id: usize) -> Result<bool> {
        if let Some(ExecutionStage::Resolved(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::UnResolved(stage.to_unresolved()?));
            Ok(true)
        } else {
            warn!(
                "Fail to find a resolved stage {}/{} to rollback",
                self.job_id(),
                stage_id
            );
            Ok(false)
        }
    }

    /// Convert successful stage to be running
    pub fn rerun_successful_stage(&mut self, stage_id: usize) -> bool {
        if let Some(ExecutionStage::Successful(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Running(stage.to_running()));
            true
        } else {
            warn!(
                "Fail to find a successful stage {}/{} to rerun",
                self.job_id(),
                stage_id
            );
            false
        }
    }

    /// fail job with error message
    pub fn fail_job(&mut self, error: String) {
        self.status = JobStatus {
            job_id: self.job_id.clone(),
            job_name: self.job_name.clone(),
            status: Some(Status::Failed(FailedJob {
                error,
                queued_at: self.queued_at,
                started_at: self.start_time,
                ended_at: self.end_time,
            })),
        };
    }

    /// Mark the job success
    pub fn succeed_job(&mut self) -> Result<()> {
        if !self.is_successful() {
            return Err(BallistaError::Internal(format!(
                "Attempt to finalize an incomplete job {}",
                self.job_id()
            )));
        }

        let partition_location = self
            .output_locations()
            .into_iter()
            .map(|l| l.try_into())
            .collect::<Result<Vec<_>>>()?;

        self.status = JobStatus {
            job_id: self.job_id.clone(),
            job_name: self.job_name.clone(),
            status: Some(job_status::Status::Successful(SuccessfulJob {
                partition_location,

                queued_at: self.queued_at,
                started_at: self.start_time,
                ended_at: self.end_time,
            })),
        };
        self.end_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(())
    }

    /// Clear the stage failure count for this stage if the stage is finally success
    fn clear_stage_failure(&mut self, stage_id: usize) {
        self.failed_stage_attempts.remove(&stage_id);
    }

    pub(crate) async fn decode_execution_graph<
        T: 'static + AsLogicalPlan,
        U: 'static + AsExecutionPlan,
    >(
        proto: protobuf::ExecutionGraph,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
    ) -> Result<ExecutionGraph> {
        let mut stages: HashMap<usize, ExecutionStage> = HashMap::new();
        for graph_stage in proto.stages {
            let stage_type = graph_stage.stage_type.expect("Unexpected empty stage");

            let execution_stage = match stage_type {
                StageType::UnresolvedStage(stage) => {
                    let stage: UnresolvedStage =
                        UnresolvedStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::UnResolved(stage))
                }
                StageType::ResolvedStage(stage) => {
                    let stage: ResolvedStage =
                        ResolvedStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::Resolved(stage))
                }
                StageType::SuccessfulStage(stage) => {
                    let stage: SuccessfulStage =
                        SuccessfulStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::Successful(stage))
                }
                StageType::FailedStage(stage) => {
                    let stage: FailedStage =
                        FailedStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::Failed(stage))
                }
            };

            stages.insert(execution_stage.0, execution_stage.1);
        }

        let output_locations: Vec<PartitionLocation> = proto
            .output_locations
            .into_iter()
            .map(|loc| loc.try_into())
            .collect::<Result<Vec<_>>>()?;

        let failed_stage_attempts = proto
            .failed_attempts
            .into_iter()
            .map(|attempt| {
                (
                    attempt.stage_id as usize,
                    HashSet::from_iter(
                        attempt
                            .stage_attempt_num
                            .into_iter()
                            .map(|num| num as usize),
                    ),
                )
            })
            .collect();

        Ok(ExecutionGraph {
            scheduler_id: (!proto.scheduler_id.is_empty()).then_some(proto.scheduler_id),
            job_id: proto.job_id,
            job_name: proto.job_name,
            session_id: proto.session_id,
            status: proto.status.ok_or_else(|| {
                BallistaError::Internal(
                    "Invalid Execution Graph: missing job status".to_owned(),
                )
            })?,
            queued_at: proto.queued_at,
            start_time: proto.start_time,
            end_time: proto.end_time,
            stages,
            output_partitions: proto.output_partitions as usize,
            output_locations,
            task_id_gen: proto.task_id_gen as usize,
            failed_stage_attempts,
        })
    }

    /// Running stages will not be persisted so that will not be encoded.
    /// Running stages will be convert back to the resolved stages to be encoded and persisted
    pub(crate) fn encode_execution_graph<
        T: 'static + AsLogicalPlan,
        U: 'static + AsExecutionPlan,
    >(
        graph: ExecutionGraph,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::ExecutionGraph> {
        let job_id = graph.job_id().to_owned();

        let stages = graph
            .stages
            .into_values()
            .map(|stage| {
                let stage_type = match stage {
                    ExecutionStage::UnResolved(stage) => {
                        StageType::UnresolvedStage(UnresolvedStage::encode(stage, codec)?)
                    }
                    ExecutionStage::Resolved(stage) => {
                        StageType::ResolvedStage(ResolvedStage::encode(stage, codec)?)
                    }
                    ExecutionStage::Running(stage) => StageType::ResolvedStage(
                        ResolvedStage::encode(stage.to_resolved(), codec)?,
                    ),
                    ExecutionStage::Successful(stage) => StageType::SuccessfulStage(
                        SuccessfulStage::encode(job_id.clone(), stage, codec)?,
                    ),
                    ExecutionStage::Failed(stage) => StageType::FailedStage(
                        FailedStage::encode(job_id.clone(), stage, codec)?,
                    ),
                };
                Ok(protobuf::ExecutionGraphStage {
                    stage_type: Some(stage_type),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let output_locations: Vec<protobuf::PartitionLocation> = graph
            .output_locations
            .into_iter()
            .map(|loc| loc.try_into())
            .collect::<Result<Vec<_>>>()?;

        let failed_attempts: Vec<protobuf::StageAttempts> = graph
            .failed_stage_attempts
            .into_iter()
            .map(|(stage_id, attempts)| {
                let stage_attempt_num = attempts
                    .into_iter()
                    .map(|num| num as u32)
                    .collect::<Vec<_>>();
                protobuf::StageAttempts {
                    stage_id: stage_id as u32,
                    stage_attempt_num,
                }
            })
            .collect::<Vec<_>>();

        Ok(protobuf::ExecutionGraph {
            job_id: graph.job_id,
            job_name: graph.job_name,
            session_id: graph.session_id,
            status: Some(graph.status),
            queued_at: graph.queued_at,
            start_time: graph.start_time,
            end_time: graph.end_time,
            stages,
            output_partitions: graph.output_partitions as u64,
            output_locations,
            scheduler_id: graph.scheduler_id.unwrap_or_default(),
            task_id_gen: graph.task_id_gen as u32,
            failed_attempts,
        })
    }
}

impl Debug for ExecutionGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let stages = self
            .stages
            .values()
            .map(|stage| format!("{stage:?}"))
            .collect::<Vec<String>>()
            .join("");
        write!(f, "ExecutionGraph[job_id={}, session_id={}, available_tasks={}, is_successful={}]\n{}",
               self.job_id, self.session_id, self.available_tasks(), self.is_successful(), stages)
    }
}

pub(crate) fn create_task_info(executor_id: String, task_id: usize) -> TaskInfo {
    TaskInfo {
        task_id,
        scheduled_time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        // Those times will be updated when the task finish
        launch_time: 0,
        start_exec_time: 0,
        end_exec_time: 0,
        finish_time: 0,
        task_status: task_status::Status::Running(RunningTask { executor_id }),
    }
}

/// Utility for building a set of `ExecutionStage`s from
/// a list of `ShuffleWriterExec`.
///
/// This will infer the dependency structure for the stages
/// so that we can construct a DAG from the stages.
struct ExecutionStageBuilder {
    /// Stage ID which is currently being visited
    current_stage_id: usize,
    /// Map from stage ID -> List of child stage IDs
    stage_dependencies: HashMap<usize, Vec<usize>>,
    /// Map from Stage ID -> output link
    output_links: HashMap<usize, Vec<usize>>,
}

impl ExecutionStageBuilder {
    pub fn new() -> Self {
        Self {
            current_stage_id: 0,
            stage_dependencies: HashMap::new(),
            output_links: HashMap::new(),
        }
    }

    pub fn build(
        mut self,
        stages: Vec<Arc<ShuffleWriterExec>>,
    ) -> Result<HashMap<usize, ExecutionStage>> {
        let mut execution_stages: HashMap<usize, ExecutionStage> = HashMap::new();
        // First, build the dependency graph
        for stage in &stages {
            accept(stage.as_ref(), &mut self)?;
        }

        // Now, create the execution stages
        for stage in stages {
            let stage_id = stage.stage_id();
            let output_links = self.output_links.remove(&stage_id).unwrap_or_default();

            let child_stages = self
                .stage_dependencies
                .remove(&stage_id)
                .unwrap_or_default();

            let stage = if child_stages.is_empty() {
                ExecutionStage::Resolved(ResolvedStage::new(
                    stage_id,
                    0,
                    stage,
                    output_links,
                    HashMap::new(),
                    HashSet::new(),
                ))
            } else {
                ExecutionStage::UnResolved(UnresolvedStage::new(
                    stage_id,
                    stage,
                    output_links,
                    child_stages,
                ))
            };
            execution_stages.insert(stage_id, stage);
        }

        Ok(execution_stages)
    }
}

impl ExecutionPlanVisitor for ExecutionStageBuilder {
    type Error = BallistaError;

    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error> {
        if let Some(shuffle_write) = plan.as_any().downcast_ref::<ShuffleWriterExec>() {
            self.current_stage_id = shuffle_write.stage_id();
        } else if let Some(unresolved_shuffle) =
            plan.as_any().downcast_ref::<UnresolvedShuffleExec>()
        {
            if let Some(output_links) =
                self.output_links.get_mut(&unresolved_shuffle.stage_id)
            {
                if !output_links.contains(&self.current_stage_id) {
                    output_links.push(self.current_stage_id);
                }
            } else {
                self.output_links
                    .insert(unresolved_shuffle.stage_id, vec![self.current_stage_id]);
            }

            if let Some(deps) = self.stage_dependencies.get_mut(&self.current_stage_id) {
                if !deps.contains(&unresolved_shuffle.stage_id) {
                    deps.push(unresolved_shuffle.stage_id);
                }
            } else {
                self.stage_dependencies
                    .insert(self.current_stage_id, vec![unresolved_shuffle.stage_id]);
            }
        }
        Ok(true)
    }
}

/// Represents the basic unit of work for the Ballista executor. Will execute
/// one partition of one stage on one task slot.
#[derive(Clone)]
pub struct TaskDescription {
    pub session_id: String,
    pub partition: PartitionId,
    pub stage_attempt_num: usize,
    pub task_id: usize,
    pub task_attempt: usize,
    pub data_cache: bool,
    pub plan: Arc<dyn ExecutionPlan>,
}

impl Debug for TaskDescription {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);
        write!(
            f,
            "TaskDescription[session_id: {},job: {}, stage: {}.{}, partition: {} task_id {}, task attempt {}, data cache {}]\n{}",
            self.session_id,
            self.partition.job_id,
            self.partition.stage_id,
            self.stage_attempt_num,
            self.partition.partition_id,
            self.task_id,
            self.task_attempt,
            self.data_cache,
            plan
        )
    }
}

impl TaskDescription {
    pub fn get_output_partition_number(&self) -> usize {
        let shuffle_writer = self
            .plan
            .as_any()
            .downcast_ref::<ShuffleWriterExec>()
            .unwrap();
        shuffle_writer
            .shuffle_output_partitioning()
            .map(|partitioning| partitioning.partition_count())
            .unwrap_or_else(|| 1)
    }
}

fn partition_to_location(
    job_id: &str,
    map_partition_id: usize,
    stage_id: usize,
    executor: &ExecutorMetadata,
    shuffles: Vec<ShuffleWritePartition>,
) -> Vec<PartitionLocation> {
    shuffles
        .into_iter()
        .map(|shuffle| PartitionLocation {
            map_partition_id,
            partition_id: PartitionId {
                job_id: job_id.to_owned(),
                stage_id,
                partition_id: shuffle.partition_id as usize,
            },
            executor_meta: executor.clone(),
            partition_stats: PartitionStats::new(
                Some(shuffle.num_rows),
                Some(shuffle.num_batches),
                Some(shuffle.num_bytes),
            ),
            path: shuffle.path,
        })
        .collect()
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use crate::scheduler_server::event::QueryStageSchedulerEvent;
    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::{
        self, failed_task, job_status, ExecutionError, FailedTask, FetchPartitionError,
        IoError, JobStatus, TaskKilled,
    };

    use crate::state::execution_graph::ExecutionGraph;
    use crate::test_utils::{
        mock_completed_task, mock_executor, mock_failed_task, test_aggregation_plan,
        test_coalesce_plan, test_join_plan, test_two_aggregations_plan,
        test_union_all_plan, test_union_plan,
    };

    #[tokio::test]
    async fn test_drain_tasks() -> Result<()> {
        let mut agg_graph = test_aggregation_plan(4).await;

        println!("Graph: {agg_graph:?}");

        drain_tasks(&mut agg_graph)?;

        assert!(
            agg_graph.is_successful(),
            "Failed to complete aggregation plan"
        );

        let mut coalesce_graph = test_coalesce_plan(4).await;

        drain_tasks(&mut coalesce_graph)?;

        assert!(
            coalesce_graph.is_successful(),
            "Failed to complete coalesce plan"
        );

        let mut join_graph = test_join_plan(4).await;

        drain_tasks(&mut join_graph)?;

        println!("{join_graph:?}");

        assert!(join_graph.is_successful(), "Failed to complete join plan");

        let mut union_all_graph = test_union_all_plan(4).await;

        drain_tasks(&mut union_all_graph)?;

        println!("{union_all_graph:?}");

        assert!(
            union_all_graph.is_successful(),
            "Failed to complete union plan"
        );

        let mut union_graph = test_union_plan(4).await;

        drain_tasks(&mut union_graph)?;

        println!("{union_graph:?}");

        assert!(union_graph.is_successful(), "Failed to complete union plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_finalize() -> Result<()> {
        let mut agg_graph = test_aggregation_plan(4).await;

        drain_tasks(&mut agg_graph)?;

        let status = agg_graph.status();

        assert!(matches!(
            status,
            protobuf::JobStatus {
                status: Some(job_status::Status::Successful(_)),
                ..
            }
        ));

        let outputs = agg_graph.output_locations();

        assert_eq!(outputs.len(), agg_graph.output_partitions);

        for location in outputs {
            assert_eq!(location.executor_meta.host, "localhost2".to_owned());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_reset_completed_stage_executor_lost() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut join_graph = test_join_plan(4).await;

        // With the improvement of https://github.com/apache/arrow-datafusion/pull/4122,
        // unnecessary RepartitionExec can be removed
        assert_eq!(join_graph.stage_count(), 4);
        assert_eq!(join_graph.available_tasks(), 0);

        // Call revive to move the two leaf Resolved stages to Running
        join_graph.revive();

        assert_eq!(join_graph.stage_count(), 4);
        assert_eq!(join_graph.available_tasks(), 2);

        // Complete the first stage
        if let Some(task) = join_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            join_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // Complete the second stage
        if let Some(task) = join_graph.pop_next_task(&executor2.id)? {
            let task_status = mock_completed_task(task, &executor2.id);
            join_graph.update_task_status(&executor2, vec![task_status], 1, 1)?;
        }

        join_graph.revive();
        // There are 4 tasks pending schedule for the 3rd stage
        assert_eq!(join_graph.available_tasks(), 4);

        // Complete 1 task
        if let Some(task) = join_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            join_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }
        // Mock 1 running task
        let _task = join_graph.pop_next_task(&executor1.id)?;

        let reset = join_graph.reset_stages_on_lost_executor(&executor1.id)?;

        // Two stages were reset, 1 Running stage rollback to Unresolved and 1 Completed stage move to Running
        assert_eq!(reset.0.len(), 2);
        assert_eq!(join_graph.available_tasks(), 1);

        drain_tasks(&mut join_graph)?;
        assert!(join_graph.is_successful(), "Failed to complete join plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_reset_resolved_stage_executor_lost() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut join_graph = test_join_plan(4).await;

        assert_eq!(join_graph.stage_count(), 4);
        assert_eq!(join_graph.available_tasks(), 0);

        // Call revive to move the two leaf Resolved stages to Running
        join_graph.revive();

        assert_eq!(join_graph.stage_count(), 4);
        assert_eq!(join_graph.available_tasks(), 2);

        // Complete the first stage
        if let Some(task) = join_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            join_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // Complete the second stage
        if let Some(task) = join_graph.pop_next_task(&executor2.id)? {
            let task_status = mock_completed_task(task, &executor2.id);
            join_graph.update_task_status(&executor2, vec![task_status], 1, 1)?;
        }

        // There are 0 tasks pending schedule now
        assert_eq!(join_graph.available_tasks(), 0);

        let reset = join_graph.reset_stages_on_lost_executor(&executor1.id)?;

        // Two stages were reset, 1 Resolved stage rollback to Unresolved and 1 Completed stage move to Running
        assert_eq!(reset.0.len(), 2);
        assert_eq!(join_graph.available_tasks(), 1);

        drain_tasks(&mut join_graph)?;
        assert!(join_graph.is_successful(), "Failed to complete join plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_task_update_after_reset_stage() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;

        assert_eq!(agg_graph.stage_count(), 2);
        assert_eq!(agg_graph.available_tasks(), 0);

        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        assert_eq!(agg_graph.stage_count(), 2);
        assert_eq!(agg_graph.available_tasks(), 1);

        // Complete the first stage
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // 1st task in the second stage
        if let Some(task) = agg_graph.pop_next_task(&executor2.id)? {
            let task_status = mock_completed_task(task, &executor2.id);
            agg_graph.update_task_status(&executor2, vec![task_status], 1, 1)?;
        }

        // 2rd task in the second stage
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // 3rd task in the second stage, scheduled but not completed
        let task = agg_graph.pop_next_task(&executor1.id)?;

        // There is 1 task pending schedule now
        assert_eq!(agg_graph.available_tasks(), 1);

        let reset = agg_graph.reset_stages_on_lost_executor(&executor1.id)?;

        // 3rd task status update comes later.
        let task_status = mock_completed_task(task.unwrap(), &executor1.id);
        agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;

        // Two stages were reset, 1 Running stage rollback to Unresolved and 1 Completed stage move to Running
        assert_eq!(reset.0.len(), 2);
        assert_eq!(agg_graph.available_tasks(), 1);

        // Call the reset again
        let reset = agg_graph.reset_stages_on_lost_executor(&executor1.id)?;
        assert_eq!(reset.0.len(), 0);
        assert_eq!(agg_graph.available_tasks(), 1);

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_do_not_retry_killed_task() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        // Complete the first stage
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // 1st task in the second stage
        let task1 = agg_graph.pop_next_task(&executor2.id)?.unwrap();
        let task_status1 = mock_completed_task(task1, &executor2.id);

        // 2rd task in the second stage
        let task2 = agg_graph.pop_next_task(&executor2.id)?.unwrap();
        let task_status2 = mock_failed_task(
            task2,
            FailedTask {
                error: "Killed".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::TaskKilled(TaskKilled {})),
            },
        );

        agg_graph.update_task_status(
            &executor2,
            vec![task_status1, task_status2],
            4,
            4,
        )?;

        assert_eq!(agg_graph.available_tasks(), 2);
        drain_tasks(&mut agg_graph)?;
        assert_eq!(agg_graph.available_tasks(), 0);

        assert!(
            !agg_graph.is_successful(),
            "Expected the agg graph can not complete"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_max_task_failed_count() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(2).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        // Complete the first stage
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // 1st task in the second stage
        let task1 = agg_graph.pop_next_task(&executor2.id)?.unwrap();
        let task_status1 = mock_completed_task(task1, &executor2.id);

        // 2rd task in the second stage, failed due to IOError
        let task2 = agg_graph.pop_next_task(&executor2.id)?.unwrap();
        let task_status2 = mock_failed_task(
            task2.clone(),
            FailedTask {
                error: "IOError".to_string(),
                retryable: true,
                count_to_failures: true,
                failed_reason: Some(failed_task::FailedReason::IoError(IoError {})),
            },
        );

        agg_graph.update_task_status(
            &executor2,
            vec![task_status1, task_status2],
            4,
            4,
        )?;

        assert_eq!(agg_graph.available_tasks(), 1);

        let mut last_attempt = 0;
        // 2rd task's attempts
        for attempt in 1..5 {
            if let Some(task2_attempt) = agg_graph.pop_next_task(&executor2.id)? {
                assert_eq!(
                    task2_attempt.partition.partition_id,
                    task2.partition.partition_id
                );
                assert_eq!(task2_attempt.task_attempt, attempt);
                last_attempt = task2_attempt.task_attempt;
                let task_status = mock_failed_task(
                    task2_attempt.clone(),
                    FailedTask {
                        error: "IOError".to_string(),
                        retryable: true,
                        count_to_failures: true,
                        failed_reason: Some(failed_task::FailedReason::IoError(
                            IoError {},
                        )),
                    },
                );
                agg_graph.update_task_status(&executor2, vec![task_status], 4, 4)?;
            }
        }

        assert!(
            matches!(
                agg_graph.status,
                JobStatus {
                    status: Some(job_status::Status::Failed(_)),
                    ..
                }
            ),
            "Expected job status to be Failed"
        );

        assert_eq!(last_attempt, 3);

        let failure_reason = format!("{:?}", agg_graph.status);
        assert!(failure_reason.contains("Task 1 in Stage 2 failed 4 times, fail the stage, most recent failure reason"));
        assert!(failure_reason.contains("IOError"));
        assert!(!agg_graph.is_successful());

        Ok(())
    }

    #[tokio::test]
    async fn test_long_delayed_failed_task_after_executor_lost() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // 1st task in the Stage 2
        if let Some(task) = agg_graph.pop_next_task(&executor2.id)? {
            let task_status = mock_completed_task(task, &executor2.id);
            agg_graph.update_task_status(&executor2, vec![task_status], 1, 1)?;
        }

        // 2rd task in the Stage 2
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // 3rd task in the Stage 2, scheduled on executor 2 but not completed
        let task = agg_graph.pop_next_task(&executor2.id)?;

        // There is 1 task pending schedule now
        assert_eq!(agg_graph.available_tasks(), 1);

        // executor 1 lost
        let reset = agg_graph.reset_stages_on_lost_executor(&executor1.id)?;

        // Two stages were reset, Stage 2 rollback to Unresolved and Stage 1 move to Running
        assert_eq!(reset.0.len(), 2);
        assert_eq!(agg_graph.available_tasks(), 1);

        // Complete the Stage 1 again
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // Stage 2 move to Running
        agg_graph.revive();
        assert_eq!(agg_graph.available_tasks(), 4);

        // 3rd task in Stage 2 update comes very late due to runtime execution error.
        let task_status = mock_failed_task(
            task.unwrap(),
            FailedTask {
                error: "ExecutionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::ExecutionError(
                    ExecutionError {},
                )),
            },
        );

        // This long delayed failed task should not failure the stage/job and should not trigger any query stage events
        let query_stage_events =
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        assert!(query_stage_events.is_empty());

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_normal_fetch_failure() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // 1st task in the Stage 2
        let task1 = agg_graph.pop_next_task(&executor2.id)?.unwrap();
        let task_status1 = mock_completed_task(task1, &executor2.id);

        // 2nd task in the Stage 2, failed due to FetchPartitionError
        let task2 = agg_graph.pop_next_task(&executor2.id)?.unwrap();
        let task_status2 = mock_failed_task(
            task2,
            FailedTask {
                error: "FetchPartitionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor1.id.clone(),
                        map_stage_id: 1,
                        map_partition_id: 0,
                    },
                )),
            },
        );

        let mut running_task_count = 0;
        while let Some(_task) = agg_graph.pop_next_task(&executor2.id)? {
            running_task_count += 1;
        }
        assert_eq!(running_task_count, 2);

        let stage_events = agg_graph.update_task_status(
            &executor2,
            vec![task_status1, task_status2],
            4,
            4,
        )?;

        assert_eq!(stage_events.len(), 1);
        assert!(matches!(
            stage_events[0],
            QueryStageSchedulerEvent::CancelTasks(_)
        ));

        // Stage 1 is running
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 1);
        assert_eq!(agg_graph.available_tasks(), 1);

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");
        Ok(())
    }

    #[tokio::test]
    async fn test_many_fetch_failures_in_one_stage() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let executor3 = mock_executor("executor-id3".to_string());
        let mut agg_graph = test_two_aggregations_plan(8).await;

        agg_graph.revive();
        assert_eq!(agg_graph.stage_count(), 3);

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // Complete the Stage 2, 5 tasks run on executor_2 and 3 tasks run on executor_1
        for _i in 0..5 {
            if let Some(task) = agg_graph.pop_next_task(&executor2.id)? {
                let task_status = mock_completed_task(task, &executor2.id);
                agg_graph.update_task_status(&executor2, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 3);
        for _i in 0..3 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }

        // Run Stage 3, 6 tasks failed due to FetchPartitionError on different map partitions on executor_2
        let mut many_fetch_failure_status = vec![];
        for part in 2..8 {
            if let Some(task) = agg_graph.pop_next_task(&executor3.id)? {
                let task_status = mock_failed_task(
                    task,
                    FailedTask {
                        error: "FetchPartitionError".to_string(),
                        retryable: false,
                        count_to_failures: false,
                        failed_reason: Some(
                            failed_task::FailedReason::FetchPartitionError(
                                FetchPartitionError {
                                    executor_id: executor2.id.clone(),
                                    map_stage_id: 2,
                                    map_partition_id: part,
                                },
                            ),
                        ),
                    },
                );
                many_fetch_failure_status.push(task_status);
            }
        }
        assert_eq!(many_fetch_failure_status.len(), 6);
        agg_graph.update_task_status(&executor3, many_fetch_failure_status, 4, 4)?;

        // The Running stage should be Stage 2 now
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 5);

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");
        Ok(())
    }

    #[tokio::test]
    async fn test_many_consecutive_stage_fetch_failures() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        for attempt in 0..6 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }

            // 1rd task in the Stage 2, failed due to FetchPartitionError
            if let Some(task1) = agg_graph.pop_next_task(&executor2.id)? {
                let task_status1 = mock_failed_task(
                    task1.clone(),
                    FailedTask {
                        error: "FetchPartitionError".to_string(),
                        retryable: false,
                        count_to_failures: false,
                        failed_reason: Some(
                            failed_task::FailedReason::FetchPartitionError(
                                FetchPartitionError {
                                    executor_id: executor1.id.clone(),
                                    map_stage_id: 1,
                                    map_partition_id: 0,
                                },
                            ),
                        ),
                    },
                );

                let stage_events =
                    agg_graph.update_task_status(&executor2, vec![task_status1], 4, 4)?;

                if attempt < 3 {
                    // No JobRunningFailed stage events
                    assert_eq!(stage_events.len(), 0);
                    // Stage 1 is running
                    let running_stage = agg_graph.running_stages();
                    assert_eq!(running_stage.len(), 1);
                    assert_eq!(running_stage[0], 1);
                    assert_eq!(agg_graph.available_tasks(), 1);
                } else {
                    // Job is failed after exceeds the max_stage_failures
                    assert_eq!(stage_events.len(), 1);
                    assert!(matches!(
                        stage_events[0],
                        QueryStageSchedulerEvent::JobRunningFailed { .. }
                    ));
                    // Stage 2 is still running
                    let running_stage = agg_graph.running_stages();
                    assert_eq!(running_stage.len(), 1);
                    assert_eq!(running_stage[0], 2);
                }
            }
        }

        drain_tasks(&mut agg_graph)?;
        assert!(!agg_graph.is_successful(), "Expect to fail the agg plan");

        let failure_reason = format!("{:?}", agg_graph.status);
        assert!(failure_reason.contains("Job failed due to stage 2 failed: Stage 2 has failed 4 times, most recent failure reason"));
        assert!(failure_reason.contains("FetchPartitionError"));

        Ok(())
    }

    #[tokio::test]
    async fn test_long_delayed_fetch_failures() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let executor3 = mock_executor("executor-id3".to_string());
        let mut agg_graph = test_two_aggregations_plan(8).await;

        agg_graph.revive();
        assert_eq!(agg_graph.stage_count(), 3);

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // Complete the Stage 2, 5 tasks run on executor_2, 2 tasks run on executor_1, 1 task runs on executor_3
        for _i in 0..5 {
            if let Some(task) = agg_graph.pop_next_task(&executor2.id)? {
                let task_status = mock_completed_task(task, &executor2.id);
                agg_graph.update_task_status(&executor2, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 3);

        for _i in 0..2 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }

        if let Some(task) = agg_graph.pop_next_task(&executor3.id)? {
            let task_status = mock_completed_task(task, &executor3.id);
            agg_graph.update_task_status(&executor3, vec![task_status], 4, 4)?;
        }
        assert_eq!(agg_graph.available_tasks(), 0);

        //Run Stage 3
        // 1st task scheduled
        let task_1 = agg_graph.pop_next_task(&executor3.id)?.unwrap();
        // 2nd task scheduled
        let task_2 = agg_graph.pop_next_task(&executor3.id)?.unwrap();
        // 3rd task scheduled
        let task_3 = agg_graph.pop_next_task(&executor3.id)?.unwrap();
        // 4th task scheduled
        let task_4 = agg_graph.pop_next_task(&executor3.id)?.unwrap();
        // 5th task scheduled
        let task_5 = agg_graph.pop_next_task(&executor3.id)?.unwrap();

        // Stage 3, 1st task failed due to FetchPartitionError(executor2)
        let task_status_1 = mock_failed_task(
            task_1,
            FailedTask {
                error: "FetchPartitionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor2.id.clone(),
                        map_stage_id: 2,
                        map_partition_id: 0,
                    },
                )),
            },
        );
        agg_graph.update_task_status(&executor3, vec![task_status_1], 4, 4)?;

        // The Running stage is Stage 2 now
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 5);

        // Stage 3, 2nd task failed due to FetchPartitionError(executor2)
        let task_status_2 = mock_failed_task(
            task_2,
            FailedTask {
                error: "FetchPartitionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor2.id.clone(),
                        map_stage_id: 2,
                        map_partition_id: 1,
                    },
                )),
            },
        );
        // This task update should be ignored
        agg_graph.update_task_status(&executor3, vec![task_status_2], 4, 4)?;
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 5);

        // Stage 3, 3rd task failed due to FetchPartitionError(executor1)
        let task_status_3 = mock_failed_task(
            task_3,
            FailedTask {
                error: "FetchPartitionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor1.id.clone(),
                        map_stage_id: 2,
                        map_partition_id: 1,
                    },
                )),
            },
        );
        // This task update should be handled because it has a different failure reason
        agg_graph.update_task_status(&executor3, vec![task_status_3], 4, 4)?;
        // Running stage is still Stage 2, but available tasks changed to 7
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 7);

        // Finish 4 tasks in Stage 2, to make some progress
        for _i in 0..4 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 3);

        // Stage 3, 4th task failed due to FetchPartitionError(executor1)
        let task_status_4 = mock_failed_task(
            task_4,
            FailedTask {
                error: "FetchPartitionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor1.id.clone(),
                        map_stage_id: 2,
                        map_partition_id: 1,
                    },
                )),
            },
        );
        // This task update should be ignored because the same failure reason is already handled
        agg_graph.update_task_status(&executor3, vec![task_status_4], 4, 4)?;
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 3);

        // Finish the other 3 tasks in Stage 2
        for _i in 0..3 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 0);

        // Stage 3, the very long delayed 5th task failed due to FetchPartitionError(executor3)
        // Although the failure reason is new, but this task should be ignored
        // Because its map stage's new attempt is finished and this stage's new attempt is running
        let task_status_5 = mock_failed_task(
            task_5,
            FailedTask {
                error: "FetchPartitionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor3.id.clone(),
                        map_stage_id: 2,
                        map_partition_id: 1,
                    },
                )),
            },
        );
        agg_graph.update_task_status(&executor3, vec![task_status_5], 4, 4)?;
        // Stage 3's new attempt is running
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 3);
        assert_eq!(agg_graph.available_tasks(), 8);

        // There is one failed stage attempts: Stage 3. Stage 2 does not count to failed attempts
        assert_eq!(agg_graph.failed_stage_attempts.len(), 1);
        assert_eq!(
            agg_graph.failed_stage_attempts.get(&3).cloned(),
            Some(HashSet::from([0]))
        );
        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");
        // Failed stage attempts are cleaned
        assert_eq!(agg_graph.failed_stage_attempts.len(), 0);

        Ok(())
    }

    #[tokio::test]
    // This test case covers a race condition in delayed fetch failure handling:
    // TaskStatus of input stage's new attempt come together with the parent stage's delayed FetchFailure
    async fn test_long_delayed_fetch_failures_race_condition() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let executor3 = mock_executor("executor-id3".to_string());
        let mut agg_graph = test_two_aggregations_plan(8).await;

        agg_graph.revive();
        assert_eq!(agg_graph.stage_count(), 3);

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // Complete the Stage 2, 5 tasks run on executor_2, 3 tasks run on executor_1
        for _i in 0..5 {
            if let Some(task) = agg_graph.pop_next_task(&executor2.id)? {
                let task_status = mock_completed_task(task, &executor2.id);
                agg_graph.update_task_status(&executor2, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 3);

        for _i in 0..3 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 0);

        // Run Stage 3
        // 1st task scheduled
        let task_1 = agg_graph.pop_next_task(&executor3.id)?.unwrap();
        // 2nd task scheduled
        let task_2 = agg_graph.pop_next_task(&executor3.id)?.unwrap();

        // Stage 3, 1st task failed due to FetchPartitionError(executor2)
        let task_status_1 = mock_failed_task(
            task_1,
            FailedTask {
                error: "FetchPartitionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor2.id.clone(),
                        map_stage_id: 2,
                        map_partition_id: 0,
                    },
                )),
            },
        );
        agg_graph.update_task_status(&executor3, vec![task_status_1], 4, 4)?;

        // The Running stage is Stage 2 now
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 5);

        // Complete the 5 tasks in Stage 2's new attempts
        let mut task_status_vec = vec![];
        for _i in 0..5 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
                task_status_vec.push(mock_completed_task(task, &executor1.id))
            }
        }

        // Stage 3, 2nd task failed due to FetchPartitionError(executor1)
        let task_status_2 = mock_failed_task(
            task_2,
            FailedTask {
                error: "FetchPartitionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor1.id.clone(),
                        map_stage_id: 2,
                        map_partition_id: 1,
                    },
                )),
            },
        );
        task_status_vec.push(task_status_2);

        // TaskStatus of Stage 2 come together with Stage 3 delayed FetchFailure update.
        // The successful tasks from Stage 2 would try to succeed the Stage2 and the delayed fetch failure try to reset the TaskInfo
        agg_graph.update_task_status(&executor3, task_status_vec, 4, 4)?;
        //The Running stage is still Stage 2, 3 new pending tasks added due to FetchPartitionError(executor1)
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 3);

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_failures_in_different_stages() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let executor3 = mock_executor("executor-id3".to_string());
        let mut agg_graph = test_two_aggregations_plan(8).await;

        agg_graph.revive();
        assert_eq!(agg_graph.stage_count(), 3);

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // Complete the Stage 2, 5 tasks run on executor_2, 3 tasks run on executor_1
        for _i in 0..5 {
            if let Some(task) = agg_graph.pop_next_task(&executor2.id)? {
                let task_status = mock_completed_task(task, &executor2.id);
                agg_graph.update_task_status(&executor2, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 3);
        for _i in 0..3 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 0);

        // Run Stage 3
        // 1rd task in the Stage 3, failed due to FetchPartitionError(executor1)
        if let Some(task1) = agg_graph.pop_next_task(&executor3.id)? {
            let task_status1 = mock_failed_task(
                task1,
                FailedTask {
                    error: "FetchPartitionError".to_string(),
                    retryable: false,
                    count_to_failures: false,
                    failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                        FetchPartitionError {
                            executor_id: executor1.id.clone(),
                            map_stage_id: 2,
                            map_partition_id: 0,
                        },
                    )),
                },
            );

            let _stage_events =
                agg_graph.update_task_status(&executor3, vec![task_status1], 4, 4)?;
        }
        // The Running stage is Stage 2 now
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 3);

        // 1rd task in the Stage 2's new attempt, failed due to FetchPartitionError(executor1)
        if let Some(task1) = agg_graph.pop_next_task(&executor3.id)? {
            let task_status1 = mock_failed_task(
                task1,
                FailedTask {
                    error: "FetchPartitionError".to_string(),
                    retryable: false,
                    count_to_failures: false,
                    failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                        FetchPartitionError {
                            executor_id: executor1.id.clone(),
                            map_stage_id: 1,
                            map_partition_id: 0,
                        },
                    )),
                },
            );
            let _stage_events =
                agg_graph.update_task_status(&executor3, vec![task_status1], 4, 4)?;
        }
        // The Running stage is Stage 1 now
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 1);
        assert_eq!(agg_graph.available_tasks(), 1);

        // There are two failed stage attempts: Stage 2 and Stage 3
        assert_eq!(agg_graph.failed_stage_attempts.len(), 2);
        assert_eq!(
            agg_graph.failed_stage_attempts.get(&2).cloned(),
            Some(HashSet::from([1]))
        );
        assert_eq!(
            agg_graph.failed_stage_attempts.get(&3).cloned(),
            Some(HashSet::from([0]))
        );

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");
        assert_eq!(agg_graph.failed_stage_attempts.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_failure_with_normal_task_failure() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // 1st task in the Stage 2
        let task1 = agg_graph.pop_next_task(&executor2.id)?.unwrap();
        let task_status1 = mock_completed_task(task1, &executor2.id);

        // 2nd task in the Stage 2, failed due to FetchPartitionError
        let task2 = agg_graph.pop_next_task(&executor2.id)?.unwrap();
        let task_status2 = mock_failed_task(
            task2,
            FailedTask {
                error: "FetchPartitionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor1.id.clone(),
                        map_stage_id: 1,
                        map_partition_id: 0,
                    },
                )),
            },
        );

        // 3rd task in the Stage 2, failed due to ExecutionError
        let task3 = agg_graph.pop_next_task(&executor2.id)?.unwrap();
        let task_status3 = mock_failed_task(
            task3,
            FailedTask {
                error: "ExecutionError".to_string(),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::ExecutionError(
                    ExecutionError {},
                )),
            },
        );

        let stage_events = agg_graph.update_task_status(
            &executor2,
            vec![task_status1, task_status2, task_status3],
            4,
            4,
        )?;

        assert_eq!(stage_events.len(), 1);
        assert!(matches!(
            stage_events[0],
            QueryStageSchedulerEvent::JobRunningFailed { .. }
        ));

        drain_tasks(&mut agg_graph)?;
        assert!(!agg_graph.is_successful(), "Expect to fail the agg plan");

        let failure_reason = format!("{:?}", agg_graph.status);
        assert!(failure_reason.contains("Job failed due to stage 2 failed"));
        assert!(failure_reason.contains("ExecutionError"));

        Ok(())
    }

    // #[tokio::test]
    // async fn test_shuffle_files_should_cleaned_after_fetch_failure() -> Result<()> {
    //     todo!()
    // }

    fn drain_tasks(graph: &mut ExecutionGraph) -> Result<()> {
        let executor = mock_executor("executor-id1".to_string());
        while let Some(task) = graph.pop_next_task(&executor.id)? {
            let task_status = mock_completed_task(task, &executor.id);
            graph.update_task_status(&executor, vec![task_status], 1, 1)?;
        }

        Ok(())
    }
}
