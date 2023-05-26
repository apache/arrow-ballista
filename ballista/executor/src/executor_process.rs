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

//! Ballista Executor Process

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, io};

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{error, info, warn};
use tempfile::TempDir;
use tokio::fs::DirEntry;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::{fs, time};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};

use ballista_core::config::{LogRotationPolicy, TaskSchedulingPolicy};
use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf::executor_resource::Resource;
use ballista_core::serde::protobuf::executor_status::Status;
use ballista_core::serde::protobuf::{
    executor_registration, scheduler_grpc_client::SchedulerGrpcClient,
    ExecutorRegistration, ExecutorResource, ExecutorSpecification, ExecutorStatus,
    ExecutorStoppedParams, HeartBeatParams,
};
use ballista_core::serde::BallistaCodec;
use ballista_core::terminate;
use ballista_core::utils::{
    create_grpc_client_connection, create_grpc_server, with_object_store_provider,
};
use ballista_core::BALLISTA_VERSION;

use crate::execution_engine::ExecutionEngine;
use crate::executor::{Executor, TasksDrainedFuture};
use crate::executor_server::TERMINATING;
use crate::flight_service::BallistaFlightService;
use crate::metrics::LoggingMetricsCollector;
use crate::shutdown::Shutdown;
use crate::shutdown::ShutdownNotifier;
use crate::{execution_loop, executor_server};

pub struct ExecutorProcessConfig {
    pub bind_host: String,
    pub external_host: Option<String>,
    pub port: u16,
    pub grpc_port: u16,
    pub scheduler_host: String,
    pub scheduler_port: u16,
    pub scheduler_connect_timeout_seconds: u16,
    pub concurrent_tasks: usize,
    pub task_scheduling_policy: TaskSchedulingPolicy,
    pub log_dir: Option<String>,
    pub work_dir: Option<String>,
    pub special_mod_log_level: String,
    pub print_thread_info: bool,
    pub log_file_name_prefix: String,
    pub log_rotation_policy: LogRotationPolicy,
    pub job_data_ttl_seconds: u64,
    pub job_data_clean_up_interval_seconds: u64,
    /// Optional execution engine to use to execute physical plans, will default to
    /// DataFusion if none is provided.
    pub execution_engine: Option<Arc<dyn ExecutionEngine>>,
}

pub async fn start_executor_process(opt: ExecutorProcessConfig) -> Result<()> {
    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or(opt.special_mod_log_level));
    // File layer
    if let Some(log_dir) = opt.log_dir {
        let log_file = match opt.log_rotation_policy {
            LogRotationPolicy::Minutely => {
                tracing_appender::rolling::minutely(log_dir, &opt.log_file_name_prefix)
            }
            LogRotationPolicy::Hourly => {
                tracing_appender::rolling::hourly(log_dir, &opt.log_file_name_prefix)
            }
            LogRotationPolicy::Daily => {
                tracing_appender::rolling::daily(log_dir, &opt.log_file_name_prefix)
            }
            LogRotationPolicy::Never => {
                tracing_appender::rolling::never(log_dir, &opt.log_file_name_prefix)
            }
        };
        tracing_subscriber::fmt()
            .with_ansi(true)
            .with_thread_names(opt.print_thread_info)
            .with_thread_ids(opt.print_thread_info)
            .with_writer(log_file)
            .with_env_filter(log_filter)
            .init();
    } else {
        // Console layer
        tracing_subscriber::fmt()
            .with_ansi(true)
            .with_thread_names(opt.print_thread_info)
            .with_thread_ids(opt.print_thread_info)
            .with_writer(io::stdout)
            .with_env_filter(log_filter)
            .init();
    }

    let addr = format!("{}:{}", opt.bind_host, opt.port);
    let addr = addr
        .parse()
        .with_context(|| format!("Could not parse address: {addr}"))?;

    let scheduler_host = opt.scheduler_host;
    let scheduler_port = opt.scheduler_port;
    let scheduler_url = format!("http://{scheduler_host}:{scheduler_port}");

    let work_dir = opt.work_dir.unwrap_or(
        TempDir::new()?
            .into_path()
            .into_os_string()
            .into_string()
            .unwrap(),
    );

    let concurrent_tasks = if opt.concurrent_tasks == 0 {
        // use all available cores if no concurrency level is specified
        num_cpus::get()
    } else {
        opt.concurrent_tasks
    };

    info!("Running with config:");
    info!("work_dir: {}", work_dir);
    info!("concurrent_tasks: {}", concurrent_tasks);

    // assign this executor an unique ID
    let executor_id = Uuid::new_v4().to_string();
    let executor_meta = ExecutorRegistration {
        id: executor_id.clone(),
        optional_host: opt
            .external_host
            .clone()
            .map(executor_registration::OptionalHost::Host),
        port: opt.port as u32,
        grpc_port: opt.grpc_port as u32,
        specification: Some(ExecutorSpecification {
            resources: vec![ExecutorResource {
                resource: Some(Resource::TaskSlots(concurrent_tasks as u32)),
            }],
        }),
    };

    let config = with_object_store_provider(
        RuntimeConfig::new().with_temp_file_path(work_dir.clone()),
    );
    let runtime = Arc::new(RuntimeEnv::new(config).map_err(|_| {
        BallistaError::Internal("Failed to init Executor RuntimeEnv".to_owned())
    })?);

    let metrics_collector = Arc::new(LoggingMetricsCollector::default());

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        runtime,
        metrics_collector,
        concurrent_tasks,
        opt.execution_engine,
    ));

    let connect_timeout = opt.scheduler_connect_timeout_seconds as u64;
    let connection = if connect_timeout == 0 {
        create_grpc_client_connection(scheduler_url)
            .await
            .context("Could not connect to scheduler")
    } else {
        // this feature was added to support docker-compose so that we can have the executor
        // wait for the scheduler to start, or at least run for 10 seconds before failing so
        // that docker-compose's restart policy will restart the container.
        let start_time = Instant::now().elapsed().as_secs();
        let mut x = None;
        while x.is_none()
            && Instant::now().elapsed().as_secs() - start_time < connect_timeout
        {
            match create_grpc_client_connection(scheduler_url.clone())
                .await
                .context("Could not connect to scheduler")
            {
                Ok(conn) => {
                    info!("Connected to scheduler at {}", scheduler_url);
                    x = Some(conn);
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to scheduler at {} ({}); retrying ...",
                        scheduler_url, e
                    );
                    std::thread::sleep(time::Duration::from_millis(500));
                }
            }
        }
        match x {
            Some(conn) => Ok(conn),
            _ => Err(BallistaError::General(format!(
                "Timed out attempting to connect to scheduler at {scheduler_url}"
            ))
            .into()),
        }
    }?;

    let mut scheduler = SchedulerGrpcClient::new(connection);

    let default_codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> =
        BallistaCodec::default();

    let scheduler_policy = opt.task_scheduling_policy;
    let job_data_ttl_seconds = opt.job_data_ttl_seconds;

    // Graceful shutdown notification
    let shutdown_noti = ShutdownNotifier::new();

    if opt.job_data_clean_up_interval_seconds > 0 {
        let mut interval_time =
            time::interval(Duration::from_secs(opt.job_data_clean_up_interval_seconds));
        let mut shuffle_cleaner_shutdown = shutdown_noti.subscribe_for_shutdown();
        let shuffle_cleaner_complete = shutdown_noti.shutdown_complete_tx.clone();
        tokio::spawn(async move {
            // As long as the shutdown notification has not been received
            while !shuffle_cleaner_shutdown.is_shutdown() {
                tokio::select! {
                    _ = interval_time.tick() => {
                            if let Err(e) = clean_shuffle_data_loop(&work_dir, job_data_ttl_seconds).await
                        {
                            error!("Ballista executor fail to clean_shuffle_data {:?}", e)
                        }
                        },
                    _ = shuffle_cleaner_shutdown.recv() => {
                        if let Err(e) = clean_all_shuffle_data(&work_dir).await
                        {
                            error!("Ballista executor fail to clean_shuffle_data {:?}", e)
                        } else {
                            info!("Shuffle data cleaned.");
                        }
                        drop(shuffle_cleaner_complete);
                        return;
                    }
                };
            }
        });
    }

    let mut service_handlers: FuturesUnordered<JoinHandle<Result<(), BallistaError>>> =
        FuturesUnordered::new();

    // Channels used to receive stop requests from Executor grpc service.
    let (stop_send, mut stop_recv) = mpsc::channel::<bool>(10);

    match scheduler_policy {
        TaskSchedulingPolicy::PushStaged => {
            service_handlers.push(
                //If there is executor registration error during startup, return the error and stop early.
                executor_server::startup(
                    scheduler.clone(),
                    opt.bind_host,
                    executor.clone(),
                    default_codec,
                    stop_send,
                    &shutdown_noti,
                )
                .await?,
            );
        }
        _ => {
            service_handlers.push(tokio::spawn(execution_loop::poll_loop(
                scheduler.clone(),
                executor.clone(),
                default_codec,
            )));
        }
    };
    service_handlers.push(tokio::spawn(flight_server_run(
        addr,
        shutdown_noti.subscribe_for_shutdown(),
    )));

    let tasks_drained = TasksDrainedFuture(executor);

    // Concurrently run the service checking and listen for the `shutdown` signal and wait for the stop request coming.
    // The check_services runs until an error is encountered, so under normal circumstances, this `select!` statement runs
    // until the `shutdown` signal is received or a stop request is coming.
    let (notify_scheduler, stop_reason) = tokio::select! {
        service_val = check_services(&mut service_handlers) => {
            let msg = format!("executor services stopped with reason {service_val:?}");
            info!("{:?}", msg);
            (true, msg)
        },
        _ = signal::ctrl_c() => {
            let msg = "executor received ctrl-c event.".to_string();
             info!("{:?}", msg);
            (true, msg)
        },
        _ = terminate::sig_term() => {
            let msg = "executor received terminate signal.".to_string();
             info!("{:?}", msg);
            (true, msg)
        },
        _ = stop_recv.recv() => {
            (false, "".to_string())
        },
    };

    // Set status to fenced
    info!("setting executor to TERMINATING status");
    TERMINATING.store(true, Ordering::Release);

    if notify_scheduler {
        // Send a heartbeat to update status of executor to `Fenced`. This should signal to the
        // scheduler to no longer schedule tasks on this executor
        if let Err(error) = scheduler
            .heart_beat_from_executor(HeartBeatParams {
                executor_id: executor_id.clone(),
                metrics: vec![],
                status: Some(ExecutorStatus {
                    status: Some(Status::Terminating(String::default())),
                }),
                metadata: Some(ExecutorRegistration {
                    id: executor_id.clone(),
                    optional_host: opt
                        .external_host
                        .clone()
                        .map(executor_registration::OptionalHost::Host),
                    port: opt.port as u32,
                    grpc_port: opt.grpc_port as u32,
                    specification: Some(ExecutorSpecification {
                        resources: vec![ExecutorResource {
                            resource: Some(Resource::TaskSlots(concurrent_tasks as u32)),
                        }],
                    }),
                }),
            })
            .await
        {
            error!("error sending heartbeat with fenced status: {:?}", error);
        }

        // TODO we probably don't need a separate rpc call for this....
        if let Err(error) = scheduler
            .executor_stopped(ExecutorStoppedParams {
                executor_id,
                reason: stop_reason,
            })
            .await
        {
            error!("ExecutorStopped grpc failed: {:?}", error);
        }

        // Wait for tasks to drain
        tasks_drained.await;
    }

    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let ShutdownNotifier {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = shutdown_noti;

    // When `notify_shutdown` is dropped, all components which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all related components to finish the shutdown processing.
    let _ = shutdown_complete_rx.recv().await;
    info!("Executor stopped.");
    Ok(())
}

// Arrow flight service
async fn flight_server_run(
    addr: SocketAddr,
    mut grpc_shutdown: Shutdown,
) -> Result<(), BallistaError> {
    let service = BallistaFlightService::new();
    let server = FlightServiceServer::new(service);
    info!(
        "Ballista v{} Rust Executor Flight Server listening on {:?}",
        BALLISTA_VERSION, addr
    );

    let shutdown_signal = grpc_shutdown.recv();
    let server_future = create_grpc_server()
        .add_service(server)
        .serve_with_shutdown(addr, shutdown_signal);

    server_future.await.map_err(|e| {
        error!("Tonic error, Could not start Executor Flight Server.");
        BallistaError::TonicError(e)
    })
}

// Check the status of long running services
async fn check_services(
    service_handlers: &mut FuturesUnordered<JoinHandle<Result<(), BallistaError>>>,
) -> Result<(), BallistaError> {
    loop {
        match service_handlers.next().await {
            Some(result) => match result {
                // React to "inner_result", i.e. propagate as BallistaError
                Ok(inner_result) => match inner_result {
                    Ok(()) => (),
                    Err(e) => return Err(e),
                },
                // React to JoinError
                Err(e) => return Err(BallistaError::TokioError(e)),
            },
            None => {
                info!("service handlers are all done with their work!");
                return Ok(());
            }
        }
    }
}

/// This function will be scheduled periodically for cleanup the job shuffle data left on the executor.
/// Only directories will be checked cleaned.
async fn clean_shuffle_data_loop(work_dir: &str, seconds: u64) -> Result<()> {
    let mut dir = fs::read_dir(work_dir).await?;
    let mut to_deleted = Vec::new();
    while let Some(child) = dir.next_entry().await? {
        if let Ok(metadata) = child.metadata().await {
            let child_path = child.path().into_os_string();
            // only delete the job dir
            if metadata.is_dir() {
                match satisfy_dir_ttl(child, seconds).await {
                    Err(e) => {
                        error!(
                            "Fail to check ttl for the directory {:?} due to {:?}",
                            child_path, e
                        )
                    }
                    Ok(false) => to_deleted.push(child_path),
                    Ok(_) => {}
                }
            } else {
                warn!("{:?} under the working directory is a not a directory and will be ignored when doing cleanup", child_path)
            }
        } else {
            error!("Fail to get metadata for file {:?}", child.path())
        }
    }
    info!(
        "The directories {:?} that have not been modified for {:?} seconds so that they will be deleted",
        to_deleted, seconds
    );
    for del in to_deleted {
        if let Err(e) = fs::remove_dir_all(&del).await {
            error!("Fail to remove the directory {:?} due to {}", del, e);
        }
    }
    Ok(())
}

/// This function will clean up all shuffle data on this executor
async fn clean_all_shuffle_data(work_dir: &str) -> Result<()> {
    let mut dir = fs::read_dir(work_dir).await?;
    let mut to_deleted = Vec::new();
    while let Some(child) = dir.next_entry().await? {
        if let Ok(metadata) = child.metadata().await {
            // only delete the job dir
            if metadata.is_dir() {
                to_deleted.push(child.path().into_os_string())
            }
        } else {
            error!("Can not get metadata from file: {:?}", child)
        }
    }

    info!("The work_dir {:?} will be deleted", &to_deleted);
    for del in to_deleted {
        if let Err(e) = fs::remove_dir_all(&del).await {
            error!("Fail to remove the directory {:?} due to {}", del, e);
        }
    }
    Ok(())
}

/// Determines if a directory contains files newer than the cutoff time.
/// If return true, it means the directory contains files newer than the cutoff time. It satisfy the ttl and should not be deleted.
pub async fn satisfy_dir_ttl(dir: DirEntry, ttl_seconds: u64) -> Result<bool> {
    let cutoff = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .checked_sub(Duration::from_secs(ttl_seconds))
        .expect("The cut off time went backwards");

    let mut to_check = vec![dir];
    while let Some(dir) = to_check.pop() {
        // Check the ttl for the current directory first
        if dir
            .metadata()
            .await?
            .modified()?
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            > cutoff
        {
            return Ok(true);
        }
        // Check its children
        let mut children = fs::read_dir(dir.path()).await?;
        while let Some(child) = children.next_entry().await? {
            let metadata = child.metadata().await?;
            if metadata.is_dir() {
                to_check.push(child);
            } else if metadata
                .modified()?
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                > cutoff
            {
                return Ok(true);
            };
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::clean_shuffle_data_loop;
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_executor_clean_up() {
        let work_dir = TempDir::new().unwrap().into_path();
        let job_dir = work_dir.as_path().join("job_id");
        let file_path = job_dir.as_path().join("tmp.csv");
        let data = "Jorge,2018-12-13T12:12:10.011Z\n\
                    Andrew,2018-11-13T17:11:10.011Z";
        fs::create_dir(job_dir).unwrap();
        File::create(&file_path)
            .expect("creating temp file")
            .write_all(data.as_bytes())
            .expect("writing data");

        let work_dir_clone = work_dir.clone();

        let count1 = fs::read_dir(work_dir.clone()).unwrap().count();
        assert_eq!(count1, 1);
        let mut handles = vec![];
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            clean_shuffle_data_loop(work_dir_clone.to_str().unwrap(), 1)
                .await
                .unwrap();
        }));
        futures::future::join_all(handles).await;
        let count2 = fs::read_dir(work_dir.clone()).unwrap().count();
        assert_eq!(count2, 0);
    }
}
