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

use ballista_core::error::{BallistaError, Result};
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::config::SchedulerConfig;
use crate::metrics::SchedulerMetricsCollector;
use crate::scheduler_server::{timestamp_millis, SchedulerServer};
use crate::state::backend::sled::SledClient;

use crate::state::executor_manager::ExecutorManager;
use crate::state::task_manager::TaskLauncher;

use ballista_core::config::{BallistaConfig, BALLISTA_DEFAULT_SHUFFLE_PARTITIONS};
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{
    task_status, JobStatus, MultiTaskDefinition, PhysicalPlanNode, ShuffleWritePartition,
    SuccessfulTask, TaskId, TaskStatus,
};
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorMetadata, ExecutorSpecification,
};
use ballista_core::serde::BallistaCodec;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::CsvReadOptions;

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::state::backend::cluster::DefaultClusterState;
use datafusion_proto::protobuf::LogicalPlanNode;
use parking_lot::Mutex;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

/// Sometimes we need to construct logical plans that will produce errors
/// when we try and create physical plan. A scan using `ExplodingTableProvider`
/// will do the trick
pub struct ExplodingTableProvider;

#[async_trait]
impl TableProvider for ExplodingTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "ExplodingTableProvider just throws an error!".to_owned(),
        ))
    }
}

/// Utility for running some async check multiple times to verify a condition. It will run the check
/// at the specified interval up to a maximum of the specified iterations.
pub async fn await_condition<Fut: Future<Output = Result<bool>>, F: Fn() -> Fut>(
    interval: Duration,
    iterations: usize,
    cond: F,
) -> Result<bool> {
    let mut iteration = 0;

    while iteration < iterations {
        let check = cond().await?;

        if check {
            return Ok(true);
        } else {
            iteration += 1;
            tokio::time::sleep(interval).await;
        }
    }

    Ok(false)
}

pub async fn datafusion_test_context(path: &str) -> Result<SessionContext> {
    let default_shuffle_partitions = 2;
    let config = SessionConfig::new().with_target_partitions(default_shuffle_partitions);
    let ctx = SessionContext::with_config(config);
    for table in TPCH_TABLES {
        let schema = get_tpch_schema(table);
        let options = CsvReadOptions::new()
            .schema(&schema)
            .delimiter(b'|')
            .has_header(false)
            .file_extension(".tbl");
        let dir = format!("{path}/{table}");
        ctx.register_csv(table, &dir, options).await?;
    }
    Ok(ctx)
}

pub fn get_tpch_schema(table: &str) -> Schema {
    // note that the schema intentionally uses signed integers so that any generated Parquet
    // files can also be used to benchmark tools that only support signed integers, such as
    // Apache Spark

    match table {
        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::Int32, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Float64, false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int32, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int32, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Float64, false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int32, false),
            Field::new("ps_suppkey", DataType::Int32, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Float64, false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int32, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int32, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Float64, false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int32, false),
            Field::new("o_custkey", DataType::Int32, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int32, false),
            Field::new("l_partkey", DataType::Int32, false),
            Field::new("l_suppkey", DataType::Int32, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int32, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int32, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int32, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}

pub trait TaskRunner: Send + Sync + 'static {
    fn run(&self, executor_id: String, tasks: MultiTaskDefinition) -> Vec<TaskStatus>;
}

#[derive(Clone)]
pub struct TaskRunnerFn<F> {
    f: F,
}

impl<F> TaskRunnerFn<F>
where
    F: Fn(String, MultiTaskDefinition) -> Vec<TaskStatus> + Send + Sync + 'static,
{
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> TaskRunner for TaskRunnerFn<F>
where
    F: Fn(String, MultiTaskDefinition) -> Vec<TaskStatus> + Send + Sync + 'static,
{
    fn run(&self, executor_id: String, tasks: MultiTaskDefinition) -> Vec<TaskStatus> {
        (self.f)(executor_id, tasks)
    }
}

pub fn default_task_runner() -> impl TaskRunner {
    TaskRunnerFn::new(|executor_id: String, task: MultiTaskDefinition| {
        let mut statuses = vec![];

        let partitions =
            if let Some(output_partitioning) = task.output_partitioning.as_ref() {
                output_partitioning.partition_count as usize
            } else {
                1
            };

        let partitions: Vec<ShuffleWritePartition> = (0..partitions)
            .into_iter()
            .map(|i| ShuffleWritePartition {
                partition_id: i as u64,
                path: String::default(),
                num_batches: 1,
                num_rows: 1,
                num_bytes: 1,
            })
            .collect();

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
                status: Some(task_status::Status::Successful(SuccessfulTask {
                    executor_id: executor_id.clone(),
                    partitions: partitions.clone(),
                })),
            });
        }

        statuses
    })
}

#[derive(Clone)]
struct VirtualExecutor {
    executor_id: String,
    task_slots: usize,
    runner: Arc<dyn TaskRunner>,
}

impl VirtualExecutor {
    pub fn run_tasks(&self, tasks: MultiTaskDefinition) -> Vec<TaskStatus> {
        self.runner.run(self.executor_id.clone(), tasks)
    }
}

/// Launcher which consumes tasks and never sends a status update
#[derive(Default)]
pub struct BlackholeTaskLauncher {}

#[async_trait]
impl TaskLauncher for BlackholeTaskLauncher {
    async fn launch_tasks(
        &self,
        _executor: &ExecutorMetadata,
        _tasks: Vec<MultiTaskDefinition>,
        _executor_manager: &ExecutorManager,
    ) -> Result<()> {
        Ok(())
    }
}

pub struct VirtualTaskLauncher {
    sender: Sender<(String, Vec<TaskStatus>)>,
    executors: HashMap<String, VirtualExecutor>,
}

#[async_trait::async_trait]
impl TaskLauncher for VirtualTaskLauncher {
    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<MultiTaskDefinition>,
        _executor_manager: &ExecutorManager,
    ) -> Result<()> {
        let virtual_executor = self.executors.get(&executor.id).ok_or_else(|| {
            BallistaError::Internal(format!(
                "No virtual executor with ID {} found",
                executor.id
            ))
        })?;

        let status = tasks
            .into_iter()
            .flat_map(|t| virtual_executor.run_tasks(t))
            .collect();

        self.sender
            .send((executor.id.clone(), status))
            .await
            .map_err(|e| {
                BallistaError::Internal(format!("Error sending task status: {e:?}"))
            })
    }
}

pub struct SchedulerTest {
    scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    ballista_config: BallistaConfig,
    status_receiver: Option<Receiver<(String, Vec<TaskStatus>)>>,
}

impl SchedulerTest {
    pub async fn new(
        config: SchedulerConfig,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
        num_executors: usize,
        task_slots_per_executor: usize,
        runner: Option<Arc<dyn TaskRunner>>,
    ) -> Result<Self> {
        let state_storage = Arc::new(SledClient::try_new_temporary()?);
        let cluster_state = Arc::new(DefaultClusterState::new(state_storage.clone()));

        let ballista_config = BallistaConfig::builder()
            .set(
                BALLISTA_DEFAULT_SHUFFLE_PARTITIONS,
                format!("{}", num_executors * task_slots_per_executor).as_str(),
            )
            .build()
            .expect("creating BallistaConfig");

        let runner = runner.unwrap_or_else(|| Arc::new(default_task_runner()));

        let executors: HashMap<String, VirtualExecutor> = (0..num_executors)
            .into_iter()
            .map(|i| {
                let id = format!("virtual-executor-{i}");
                let executor = VirtualExecutor {
                    executor_id: id.clone(),
                    task_slots: task_slots_per_executor,
                    runner: runner.clone(),
                };
                (id, executor)
            })
            .collect();

        let (status_sender, status_receiver) = channel(1000);

        let launcher = VirtualTaskLauncher {
            sender: status_sender,
            executors: executors.clone(),
        };

        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::with_task_launcher(
                "localhost:50050".to_owned(),
                state_storage,
                cluster_state,
                BallistaCodec::default(),
                config,
                metrics_collector,
                Arc::new(launcher),
            );
        scheduler.init().await?;

        for (executor_id, VirtualExecutor { task_slots, .. }) in executors {
            let metadata = ExecutorMetadata {
                id: executor_id.clone(),
                host: String::default(),
                port: 0,
                grpc_port: 0,
                specification: ExecutorSpecification {
                    task_slots: task_slots as u32,
                },
            };

            let executor_data = ExecutorData {
                executor_id,
                total_task_slots: task_slots as u32,
                available_task_slots: task_slots as u32,
            };

            scheduler
                .state
                .executor_manager
                .register_executor(metadata, executor_data, false)
                .await?;
        }

        Ok(Self {
            scheduler,
            ballista_config,
            status_receiver: Some(status_receiver),
        })
    }

    pub fn pending_tasks(&self) -> usize {
        self.scheduler.pending_tasks()
    }

    pub async fn ctx(&self) -> Result<Arc<SessionContext>> {
        self.scheduler
            .state
            .session_manager
            .create_session(&self.ballista_config)
            .await
    }

    pub async fn submit(
        &mut self,
        job_id: &str,
        job_name: &str,
        plan: &LogicalPlan,
    ) -> Result<()> {
        let ctx = self
            .scheduler
            .state
            .session_manager
            .create_session(&self.ballista_config)
            .await?;

        self.scheduler
            .submit_job(job_id, job_name, ctx, plan)
            .await?;

        Ok(())
    }

    pub async fn tick(&mut self) -> Result<()> {
        if let Some(receiver) = self.status_receiver.as_mut() {
            if let Some((executor_id, status)) = receiver.recv().await {
                self.scheduler
                    .update_task_status(&executor_id, status)
                    .await?;
            } else {
                return Err(BallistaError::Internal("Task sender dropped".to_owned()));
            }
        } else {
            return Err(BallistaError::Internal(
                "Status receiver was None".to_owned(),
            ));
        }

        Ok(())
    }

    pub async fn cancel(&self, job_id: &str) -> Result<()> {
        self.scheduler
            .query_stage_event_loop
            .get_sender()?
            .post_event(QueryStageSchedulerEvent::JobCancel(job_id.to_owned()))
            .await
    }

    pub async fn await_completion(&self, job_id: &str) -> Result<JobStatus> {
        let final_status: Result<JobStatus> = loop {
            let status = self
                .scheduler
                .state
                .task_manager
                .get_job_status(job_id)
                .await?;

            if let Some(JobStatus {
                status: Some(inner),
            }) = status.as_ref()
            {
                match inner {
                    Status::Failed(_) | Status::Successful(_) => {
                        break Ok(status.unwrap())
                    }
                    _ => continue,
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await
        };

        final_status
    }

    pub async fn run(
        &mut self,
        job_id: &str,
        job_name: &str,
        plan: &LogicalPlan,
    ) -> Result<JobStatus> {
        let ctx = self
            .scheduler
            .state
            .session_manager
            .create_session(&self.ballista_config)
            .await?;

        self.scheduler
            .submit_job(job_id, job_name, ctx, plan)
            .await?;

        let mut receiver = self.status_receiver.take().unwrap();

        let scheduler_clone = self.scheduler.clone();
        tokio::spawn(async move {
            while let Some((executor_id, status)) = receiver.recv().await {
                scheduler_clone
                    .update_task_status(&executor_id, status)
                    .await
                    .unwrap();
            }
        });

        let final_status: Result<JobStatus> = loop {
            let status = self
                .scheduler
                .state
                .task_manager
                .get_job_status(job_id)
                .await?;

            if let Some(JobStatus {
                status: Some(inner),
            }) = status.as_ref()
            {
                match inner {
                    Status::Failed(_) | Status::Successful(_) => {
                        break Ok(status.unwrap())
                    }
                    _ => continue,
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await
        };

        final_status
    }
}

#[derive(Clone)]
pub enum MetricEvent {
    Submitted(String, u64, u64),
    Completed(String, u64, u64),
    Cancelled(String),
    Failed(String, u64, u64),
}

impl MetricEvent {
    pub fn job_id(&self) -> &str {
        match self {
            MetricEvent::Submitted(job, _, _) => job.as_str(),
            MetricEvent::Completed(job, _, _) => job.as_str(),
            MetricEvent::Cancelled(job) => job.as_str(),
            MetricEvent::Failed(job, _, _) => job.as_str(),
        }
    }
}

#[derive(Default, Clone)]
pub struct TestMetricsCollector {
    pub events: Arc<Mutex<Vec<MetricEvent>>>,
}

impl TestMetricsCollector {
    pub fn job_events(&self, job_id: &str) -> Vec<MetricEvent> {
        let guard = self.events.lock();

        guard
            .iter()
            .filter_map(|event| {
                if event.job_id() == job_id {
                    Some(event.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

impl SchedulerMetricsCollector for TestMetricsCollector {
    fn record_submitted(&self, job_id: &str, queued_at: u64, submitted_at: u64) {
        let mut guard = self.events.lock();
        guard.push(MetricEvent::Submitted(
            job_id.to_owned(),
            queued_at,
            submitted_at,
        ));
    }

    fn record_completed(&self, job_id: &str, queued_at: u64, completed_at: u64) {
        let mut guard = self.events.lock();
        guard.push(MetricEvent::Completed(
            job_id.to_owned(),
            queued_at,
            completed_at,
        ));
    }

    fn record_failed(&self, job_id: &str, queued_at: u64, failed_at: u64) {
        let mut guard = self.events.lock();
        guard.push(MetricEvent::Failed(job_id.to_owned(), queued_at, failed_at));
    }

    fn record_cancelled(&self, job_id: &str) {
        let mut guard = self.events.lock();
        guard.push(MetricEvent::Cancelled(job_id.to_owned()));
    }

    fn set_pending_tasks_queue_size(&self, _value: u64) {}

    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        Ok(None)
    }
}

pub fn assert_submitted_event(job_id: &str, collector: &TestMetricsCollector) {
    let found = collector
        .job_events(job_id)
        .iter()
        .any(|ev| matches!(ev, MetricEvent::Submitted(_, _, _)));

    assert!(found, "Expected submitted event for job {}", job_id);
}

pub fn assert_no_submitted_event(job_id: &str, collector: &TestMetricsCollector) {
    let found = collector
        .job_events(job_id)
        .iter()
        .any(|ev| matches!(ev, MetricEvent::Submitted(_, _, _)));

    assert!(!found, "Expected no submitted event for job {}", job_id);
}

pub fn assert_completed_event(job_id: &str, collector: &TestMetricsCollector) {
    let found = collector
        .job_events(job_id)
        .iter()
        .any(|ev| matches!(ev, MetricEvent::Completed(_, _, _)));

    assert!(found, "Expected completed event for job {}", job_id);
}

pub fn assert_cancelled_event(job_id: &str, collector: &TestMetricsCollector) {
    let found = collector
        .job_events(job_id)
        .iter()
        .any(|ev| matches!(ev, MetricEvent::Cancelled(_)));

    assert!(found, "Expected cancelled event for job {}", job_id);
}

pub fn assert_failed_event(job_id: &str, collector: &TestMetricsCollector) {
    let found = collector
        .job_events(job_id)
        .iter()
        .any(|ev| matches!(ev, MetricEvent::Failed(_, _, _)));

    assert!(found, "Expected failed event for job {}", job_id);
}
