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

use ballista_core::BALLISTA_VERSION;
use datafusion::config::Extensions;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use log::{debug, error, info, warn};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf::{
    executor_grpc_server::{ExecutorGrpc, ExecutorGrpcServer},
    executor_metric, executor_status,
    scheduler_grpc_client::SchedulerGrpcClient,
    CancelTasksParams, CancelTasksResult, ExecutorMetric, ExecutorStatus,
    HeartBeatParams, LaunchMultiTaskParams, LaunchMultiTaskResult, LaunchTaskParams,
    LaunchTaskResult, RegisterExecutorParams, RemoveJobDataParams, RemoveJobDataResult,
    StopExecutorParams, StopExecutorResult, TaskStatus, UpdateTaskStatusParams,
};
use ballista_core::serde::scheduler::PartitionId;
use ballista_core::serde::scheduler::TaskDefinition;
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::{create_grpc_client_connection, create_grpc_server};
use dashmap::DashMap;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::{
    logical_plan::AsLogicalPlan,
    physical_plan::{from_proto::parse_protobuf_hash_partitioning, AsExecutionPlan},
};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;

use crate::cpu_bound_executor::DedicatedExecutor;
use crate::executor::Executor;
use crate::shutdown::ShutdownNotifier;
use crate::{as_task_status, TaskExecutionTimes};

type ServerHandle = JoinHandle<Result<(), BallistaError>>;
type SchedulerClients = Arc<DashMap<String, SchedulerGrpcClient<Channel>>>;

/// Wrap TaskDefinition with its curator scheduler id for task update to its specific curator scheduler later
#[derive(Debug)]
struct CuratorTaskDefinition {
    scheduler_id: String,
    task: TaskDefinition,
}

/// Wrap TaskStatus with its curator scheduler id for task update to its specific curator scheduler later
#[derive(Debug)]
struct CuratorTaskStatus {
    scheduler_id: String,
    task_status: TaskStatus,
}

pub async fn startup<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    mut scheduler: SchedulerGrpcClient<Channel>,
    bind_host: String,
    timeout: Duration,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
    stop_send: mpsc::Sender<bool>,
    shutdown_noti: &ShutdownNotifier,
) -> Result<ServerHandle, BallistaError> {
    let channel_buf_size = executor.concurrent_tasks * 50;
    let (tx_task, rx_task) = mpsc::channel::<CuratorTaskDefinition>(channel_buf_size);
    let (tx_task_status, rx_task_status) =
        mpsc::channel::<CuratorTaskStatus>(channel_buf_size);

    let executor_server = ExecutorServer::new(
        scheduler.clone(),
        executor.clone(),
        ExecutorEnv {
            tx_task,
            tx_task_status,
            tx_stop: stop_send,
        },
        codec,
    );

    // 1. Start executor grpc service
    let server = {
        let executor_meta = executor.metadata.clone();
        let addr = format!("{}:{}", bind_host, executor_meta.grpc_port);
        let addr = addr.parse().unwrap();

        info!(
            "Ballista v{} Rust Executor Grpc Server listening on {:?}",
            BALLISTA_VERSION, addr
        );
        let server = ExecutorGrpcServer::new(executor_server.clone());
        let mut grpc_shutdown = shutdown_noti.subscribe_for_shutdown();
        tokio::spawn(async move {
            let shutdown_signal = grpc_shutdown.recv();
            let grpc_server_future = create_grpc_server(timeout)
                .add_service(server)
                .serve_with_shutdown(addr, shutdown_signal);
            grpc_server_future.await.map_err(|e| {
                error!("Tonic error, Could not start Executor Grpc Server.");
                BallistaError::TonicError(e)
            })
        })
    };

    // 2. Do executor registration
    // TODO the executor registration should happen only after the executor grpc server started.
    let executor_server = Arc::new(executor_server);
    match register_executor(&mut scheduler, executor.clone()).await {
        Ok(_) => {
            info!("Executor registration succeed");
        }
        Err(error) => {
            error!("Executor registration failed due to: {}", error);
            // abort the Executor Grpc Future
            server.abort();
            return Err(error);
        }
    };

    // 3. Start Heartbeater loop
    {
        let heartbeater = Heartbeater::new(executor_server.clone());
        heartbeater.start(shutdown_noti);
    }

    // 4. Start TaskRunnerPool loop
    {
        let task_runner_pool = TaskRunnerPool::new(executor_server.clone());
        task_runner_pool.start(rx_task, rx_task_status, shutdown_noti, timeout);
    }

    Ok(server)
}

#[allow(clippy::clone_on_copy)]
async fn register_executor(
    scheduler: &mut SchedulerGrpcClient<Channel>,
    executor: Arc<Executor>,
) -> Result<(), BallistaError> {
    let result = scheduler
        .register_executor(RegisterExecutorParams {
            metadata: Some(executor.metadata.clone()),
        })
        .await?;
    if result.into_inner().success {
        Ok(())
    } else {
        Err(BallistaError::General(
            "Executor registration failed!!!".to_owned(),
        ))
    }
}

#[derive(Clone)]
pub struct ExecutorServer<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    _start_time: u128,
    executor: Arc<Executor>,
    executor_env: ExecutorEnv,
    codec: BallistaCodec<T, U>,
    scheduler_to_register: SchedulerGrpcClient<Channel>,
    schedulers: SchedulerClients,
}

#[derive(Clone)]
struct ExecutorEnv {
    /// Receive `TaskDefinition` from rpc then send to CPU bound tasks pool `dedicated_executor`.
    tx_task: mpsc::Sender<CuratorTaskDefinition>,
    /// Receive `TaskStatus` from CPU bound tasks pool `dedicated_executor` then use rpc send back to scheduler.
    tx_task_status: mpsc::Sender<CuratorTaskStatus>,
    /// Receive stop executor request from rpc.
    tx_stop: mpsc::Sender<bool>,
}

unsafe impl Sync for ExecutorEnv {}

/// Global flag indicating whether the executor is terminating. This should be
/// set to `true` when the executor receives a shutdown signal
pub static TERMINATING: AtomicBool = AtomicBool::new(false);

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExecutorServer<T, U> {
    fn new(
        scheduler_to_register: SchedulerGrpcClient<Channel>,
        executor: Arc<Executor>,
        executor_env: ExecutorEnv,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        Self {
            _start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            executor,
            executor_env,
            codec,
            scheduler_to_register,
            schedulers: Default::default(),
        }
    }

    async fn get_scheduler_client(
        &self,
        scheduler_id: &str,
        timeout: Duration,
    ) -> Result<SchedulerGrpcClient<Channel>, BallistaError> {
        let scheduler = self.schedulers.get(scheduler_id).map(|value| value.clone());
        // If channel does not exist, create a new one
        if let Some(scheduler) = scheduler {
            Ok(scheduler)
        } else {
            let scheduler_url = format!("http://{scheduler_id}");
            let connection =
                create_grpc_client_connection(scheduler_url, timeout).await?;
            let scheduler = SchedulerGrpcClient::new(connection);

            {
                self.schedulers
                    .insert(scheduler_id.to_owned(), scheduler.clone());
            }

            Ok(scheduler)
        }
    }

    /// 1. First Heartbeat to its registration scheduler, if successful then return; else go next.
    /// 2. Heartbeat to schedulers which has launching tasks to this executor until one succeeds
    async fn heartbeat(&self) {
        let status = if TERMINATING.load(Ordering::Acquire) {
            executor_status::Status::Terminating(String::default())
        } else {
            executor_status::Status::Active(String::default())
        };

        let heartbeat_params = HeartBeatParams {
            executor_id: self.executor.metadata.id.clone(),
            metrics: self.get_executor_metrics(),
            status: Some(ExecutorStatus {
                status: Some(status),
            }),
            metadata: Some(self.executor.metadata.clone()),
        };
        let mut scheduler = self.scheduler_to_register.clone();
        match scheduler
            .heart_beat_from_executor(heartbeat_params.clone())
            .await
        {
            Ok(_) => {
                return;
            }
            Err(e) => {
                warn!(
                    "Fail to update heartbeat to its registration scheduler due to {:?}",
                    e
                );
            }
        };

        for mut item in self.schedulers.iter_mut() {
            let scheduler_id = item.key().clone();
            let scheduler = item.value_mut();

            match scheduler
                .heart_beat_from_executor(heartbeat_params.clone())
                .await
            {
                Ok(_) => {
                    break;
                }
                Err(e) => {
                    warn!(
                        "Fail to update heartbeat to scheduler {} due to {:?}",
                        scheduler_id, e
                    );
                }
            }
        }
    }

    async fn run_task(
        &self,
        task_identity: String,
        curator_task: CuratorTaskDefinition,
    ) -> Result<(), BallistaError> {
        let start_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        info!("Start to run task {}", task_identity);
        let task = curator_task.task;
        let task_props = task.props;

        let mut task_scalar_functions = HashMap::new();
        let mut task_aggregate_functions = HashMap::new();
        // TODO combine the functions from Executor's functions and TaskDefintion's function resources
        for scalar_func in self.executor.scalar_functions.clone() {
            task_scalar_functions.insert(scalar_func.0, scalar_func.1);
        }
        for agg_func in self.executor.aggregate_functions.clone() {
            task_aggregate_functions.insert(agg_func.0, agg_func.1);
        }

        let session_id = task.session_id;
        let runtime = self.executor.runtime.clone();
        let task_context = Arc::new(TaskContext::try_new(
            task_identity.clone(),
            session_id,
            task_props,
            task_scalar_functions,
            task_aggregate_functions,
            runtime.clone(),
            Extensions::default(),
        )?);

        let encoded_plan = &task.plan.as_slice();

        let plan: Arc<dyn ExecutionPlan> =
            U::try_decode(encoded_plan).and_then(|proto| {
                proto.try_into_physical_plan(
                    task_context.deref(),
                    runtime.deref(),
                    self.codec.physical_extension_codec(),
                )
            })?;

        let shuffle_output_partitioning = parse_protobuf_hash_partitioning(
            task.output_partitioning.as_ref(),
            task_context.as_ref(),
            plan.schema().as_ref(),
        )?;

        let task_id = task.task_id;
        let job_id = task.job_id;
        let stage_id = task.stage_id;
        let stage_attempt_num = task.stage_attempt_num;
        let partition_id = task.partition_id;
        let query_stage_exec = self.executor.execution_engine.create_query_stage_exec(
            job_id.clone(),
            stage_id,
            plan,
            &self.executor.work_dir,
        )?;

        let part = PartitionId {
            job_id: job_id.clone(),
            stage_id,
            partition_id,
        };

        info!("Start to execute shuffle write for task {}", task_identity);

        let execution_result = self
            .executor
            .execute_query_stage(
                task_id,
                part.clone(),
                query_stage_exec.clone(),
                task_context,
                shuffle_output_partitioning,
            )
            .await;
        info!("Done with task {}", task_identity);
        debug!("Statistics: {:?}", execution_result);

        let plan_metrics = query_stage_exec.collect_plan_metrics();
        let operator_metrics = plan_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>, BallistaError>>()?;
        let executor_id = &self.executor.metadata.id;

        let end_exec_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let task_execution_times = TaskExecutionTimes {
            launch_time: task.launch_time,
            start_exec_time,
            end_exec_time,
        };

        let task_status = as_task_status(
            execution_result,
            executor_id.clone(),
            task_id,
            stage_attempt_num,
            part,
            Some(operator_metrics),
            task_execution_times,
        );

        let scheduler_id = curator_task.scheduler_id;
        let task_status_sender = self.executor_env.tx_task_status.clone();
        task_status_sender
            .send(CuratorTaskStatus {
                scheduler_id,
                task_status,
            })
            .await
            .unwrap();
        Ok(())
    }

    // TODO populate with real metrics
    fn get_executor_metrics(&self) -> Vec<ExecutorMetric> {
        let available_memory = ExecutorMetric {
            metric: Some(executor_metric::Metric::AvailableMemory(u64::MAX)),
        };
        let executor_metrics = vec![available_memory];
        executor_metrics
    }
}

/// Heartbeater will run forever until a shutdown notification received.
struct Heartbeater<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    executor_server: Arc<ExecutorServer<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> Heartbeater<T, U> {
    fn new(executor_server: Arc<ExecutorServer<T, U>>) -> Self {
        Self { executor_server }
    }

    fn start(&self, shutdown_noti: &ShutdownNotifier) {
        let executor_server = self.executor_server.clone();
        let mut heartbeat_shutdown = shutdown_noti.subscribe_for_shutdown();
        let heartbeat_complete = shutdown_noti.shutdown_complete_tx.clone();
        tokio::spawn(async move {
            info!("Starting heartbeater to send heartbeat the scheduler periodically");
            // As long as the shutdown notification has not been received
            while !heartbeat_shutdown.is_shutdown() {
                executor_server.heartbeat().await;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(60000)) => {},
                    _ = heartbeat_shutdown.recv() => {
                        info!("Stop heartbeater");
                        drop(heartbeat_complete);
                        return;
                    }
                };
            }
        });
    }
}

/// There are two loop(future) running separately in tokio runtime.
/// First is for sending back task status to scheduler
/// Second is for receiving task from scheduler and run.
/// The two loops will run forever until a shutdown notification received.
struct TaskRunnerPool<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    executor_server: Arc<ExecutorServer<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskRunnerPool<T, U> {
    fn new(executor_server: Arc<ExecutorServer<T, U>>) -> Self {
        Self { executor_server }
    }

    fn start(
        &self,
        mut rx_task: mpsc::Receiver<CuratorTaskDefinition>,
        mut rx_task_status: mpsc::Receiver<CuratorTaskStatus>,
        shutdown_noti: &ShutdownNotifier,
        timeout: Duration,
    ) {
        //1. loop for task status reporting
        let executor_server = self.executor_server.clone();
        let mut tasks_status_shutdown = shutdown_noti.subscribe_for_shutdown();
        let tasks_status_complete = shutdown_noti.shutdown_complete_tx.clone();
        tokio::spawn(async move {
            info!("Starting the task status reporter");
            // As long as the shutdown notification has not been received
            while !tasks_status_shutdown.is_shutdown() {
                let mut curator_task_status_map: HashMap<String, Vec<TaskStatus>> =
                    HashMap::new();
                // First try to fetch task status from the channel in *blocking* mode
                let maybe_task_status: Option<CuratorTaskStatus> = tokio::select! {
                     task_status = rx_task_status.recv() => task_status,
                    _ = tasks_status_shutdown.recv() => {
                        info!("Stop task status reporting loop");
                        drop(tasks_status_complete);
                        return;
                    }
                };

                let mut fetched_task_num = 0usize;
                if let Some(task_status) = maybe_task_status {
                    let task_status_vec = curator_task_status_map
                        .entry(task_status.scheduler_id)
                        .or_insert_with(Vec::new);
                    task_status_vec.push(task_status.task_status);
                    fetched_task_num += 1;
                } else {
                    info!("Channel is closed and will exit the task status report loop.");
                    drop(tasks_status_complete);
                    return;
                }

                // Then try to fetch by non-blocking mode to fetch as much finished tasks as possible
                loop {
                    match rx_task_status.try_recv() {
                        Ok(task_status) => {
                            let task_status_vec = curator_task_status_map
                                .entry(task_status.scheduler_id)
                                .or_insert_with(Vec::new);
                            task_status_vec.push(task_status.task_status);
                            fetched_task_num += 1;
                        }
                        Err(TryRecvError::Empty) => {
                            info!("Fetched {} tasks status to report", fetched_task_num);
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            info!("Channel is closed and will exit the task status report loop");
                            drop(tasks_status_complete);
                            return;
                        }
                    }
                }

                for (scheduler_id, tasks_status) in curator_task_status_map.into_iter() {
                    match executor_server
                        .get_scheduler_client(&scheduler_id, timeout)
                        .await
                    {
                        Ok(mut scheduler) => {
                            if let Err(e) = scheduler
                                .update_task_status(UpdateTaskStatusParams {
                                    executor_id: executor_server
                                        .executor
                                        .metadata
                                        .id
                                        .clone(),
                                    task_status: tasks_status.clone(),
                                })
                                .await
                            {
                                error!(
                                    "Fail to update tasks {:?} due to {:?}",
                                    tasks_status, e
                                );
                            }
                        }
                        Err(e) => {
                            error!(
                                "Fail to connect to scheduler {} due to {:?}",
                                scheduler_id, e
                            );
                        }
                    }
                }
            }
        });

        //2. loop for task fetching and running
        let executor_server = self.executor_server.clone();
        let mut task_runner_shutdown = shutdown_noti.subscribe_for_shutdown();
        let task_runner_complete = shutdown_noti.shutdown_complete_tx.clone();
        tokio::spawn(async move {
            info!("Starting the task runner pool");

            // Use a dedicated executor for CPU bound tasks so that the main tokio
            // executor can still answer requests even when under load
            let dedicated_executor = DedicatedExecutor::new(
                "task_runner",
                executor_server.executor.concurrent_tasks,
            );

            // As long as the shutdown notification has not been received
            while !task_runner_shutdown.is_shutdown() {
                let maybe_task: Option<CuratorTaskDefinition> = tokio::select! {
                     task = rx_task.recv() => task,
                    _ = task_runner_shutdown.recv() => {
                        info!("Stop the task runner pool");
                        drop(task_runner_complete);
                        return;
                    }
                };
                if let Some(curator_task) = maybe_task {
                    let task_identity = format!(
                        "TID {} {}/{}.{}/{}.{}",
                        &curator_task.task.task_id,
                        &curator_task.task.job_id,
                        &curator_task.task.stage_id,
                        &curator_task.task.stage_attempt_num,
                        &curator_task.task.partition_id,
                        &curator_task.task.task_attempt_num,
                    );
                    info!("Received task {:?}", &task_identity);

                    let server = executor_server.clone();
                    dedicated_executor.spawn(async move {
                        server
                            .run_task(task_identity.clone(), curator_task)
                            .await
                            .unwrap_or_else(|e| {
                                error!(
                                    "Fail to run the task {:?} due to {:?}",
                                    task_identity, e
                                );
                            });
                    });
                } else {
                    info!("Channel is closed and will exit the task receive loop");
                    drop(task_runner_complete);
                    return;
                }
            }
        });
    }
}

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExecutorGrpc
    for ExecutorServer<T, U>
{
    async fn launch_task(
        &self,
        request: Request<LaunchTaskParams>,
    ) -> Result<Response<LaunchTaskResult>, Status> {
        let LaunchTaskParams {
            tasks,
            scheduler_id,
        } = request.into_inner();
        let task_sender = self.executor_env.tx_task.clone();
        for task in tasks {
            task_sender
                .send(CuratorTaskDefinition {
                    scheduler_id: scheduler_id.clone(),
                    task: task
                        .try_into()
                        .map_err(|e| Status::invalid_argument(format!("{e}")))?,
                })
                .await
                .unwrap();
        }
        Ok(Response::new(LaunchTaskResult { success: true }))
    }

    /// by this interface, it can reduce the deserialization cost for multiple tasks
    /// belong to the same job stage running on the same one executor
    async fn launch_multi_task(
        &self,
        request: Request<LaunchMultiTaskParams>,
    ) -> Result<Response<LaunchMultiTaskResult>, Status> {
        let LaunchMultiTaskParams {
            multi_tasks,
            scheduler_id,
        } = request.into_inner();
        let task_sender = self.executor_env.tx_task.clone();
        for multi_task in multi_tasks {
            let multi_task: Vec<TaskDefinition> = multi_task
                .try_into()
                .map_err(|e| Status::invalid_argument(format!("{e}")))?;
            for task in multi_task {
                task_sender
                    .send(CuratorTaskDefinition {
                        scheduler_id: scheduler_id.clone(),
                        task,
                    })
                    .await
                    .unwrap();
            }
        }
        Ok(Response::new(LaunchMultiTaskResult { success: true }))
    }

    async fn stop_executor(
        &self,
        request: Request<StopExecutorParams>,
    ) -> Result<Response<StopExecutorResult>, Status> {
        let stop_request = request.into_inner();
        if stop_request.executor_id != self.executor.metadata.id {
            warn!(
                "The executor id {} in request is different from {}. The stop request will be ignored",
                stop_request.executor_id, self.executor.metadata.id
            );
            return Ok(Response::new(StopExecutorResult {}));
        }
        let stop_reason = stop_request.reason;
        let force = stop_request.force;
        info!(
            "Receive stop executor request, reason: {:?}, force {:?}",
            stop_reason, force
        );
        let stop_sender = self.executor_env.tx_stop.clone();
        stop_sender.send(force).await.unwrap();
        Ok(Response::new(StopExecutorResult {}))
    }

    async fn cancel_tasks(
        &self,
        request: Request<CancelTasksParams>,
    ) -> Result<Response<CancelTasksResult>, Status> {
        let task_infos = request.into_inner().task_infos;
        info!("Cancelling tasks for {:?}", task_infos);

        let mut cancelled = true;

        for task in task_infos {
            if let Err(e) = self
                .executor
                .cancel_task(
                    task.task_id as usize,
                    task.job_id,
                    task.stage_id as usize,
                    task.partition_id as usize,
                )
                .await
            {
                error!("Error cancelling task: {:?}", e);
                cancelled = false;
            }
        }

        Ok(Response::new(CancelTasksResult { cancelled }))
    }

    async fn remove_job_data(
        &self,
        request: Request<RemoveJobDataParams>,
    ) -> Result<Response<RemoveJobDataResult>, Status> {
        let job_id = request.into_inner().job_id;

        let work_dir = PathBuf::from(&self.executor.work_dir);
        let mut path = work_dir.clone();
        path.push(&job_id);

        // Verify it's an existing directory
        if !path.is_dir() {
            return if !path.exists() {
                Ok(Response::new(RemoveJobDataResult {}))
            } else {
                Err(Status::invalid_argument(format!(
                    "Path {path:?} is not for a directory!!!"
                )))
            };
        }

        if !is_subdirectory(path.as_path(), work_dir.as_path()) {
            return Err(Status::invalid_argument(format!(
                "Path {path:?} is not a subdirectory of {work_dir:?}!!!"
            )));
        }

        info!("Remove data for job {:?}", job_id);

        std::fs::remove_dir_all(&path)?;

        Ok(Response::new(RemoveJobDataResult {}))
    }
}

// Check whether the path is the subdirectory of the base directory
fn is_subdirectory(path: &Path, base_path: &Path) -> bool {
    if let (Ok(path), Ok(base_path)) = (path.canonicalize(), base_path.canonicalize()) {
        if let Some(parent_path) = path.parent() {
            parent_path.starts_with(base_path)
        } else {
            false
        }
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::executor_server::is_subdirectory;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_is_subdirectory() {
        let base_dir = TempDir::new().unwrap().into_path();

        // Normal correct one
        {
            let job_path = prepare_testing_job_directory(&base_dir, "job_a");
            assert!(is_subdirectory(&job_path, base_dir.as_path()));
        }

        // Empty job id
        {
            let job_path = prepare_testing_job_directory(&base_dir, "");
            assert!(!is_subdirectory(&job_path, base_dir.as_path()));

            let job_path = prepare_testing_job_directory(&base_dir, ".");
            assert!(!is_subdirectory(&job_path, base_dir.as_path()));
        }

        // Malicious job id
        {
            let job_path = prepare_testing_job_directory(&base_dir, "..");
            assert!(!is_subdirectory(&job_path, base_dir.as_path()));
        }
    }

    fn prepare_testing_job_directory(base_dir: &Path, job_id: &str) -> PathBuf {
        let mut path = base_dir.to_path_buf();
        path.push(job_id);
        if !path.exists() {
            fs::create_dir(&path).unwrap();
        }
        path
    }
}
