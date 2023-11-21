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

//! Ballista Rust scheduler binary.

use std::sync::Arc;
use std::{env, io};

use anyhow::Result;

use crate::config::{Config, ResultExt};
use ballista_core::config::LogRotationPolicy;
use ballista_core::print_version;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::cluster::ClusterStorage;
use ballista_scheduler::config::{
    ClusterStorageConfig, SchedulerConfig, TaskDistribution, TaskDistributionPolicy,
};
use ballista_scheduler::scheduler_process::start_server;
use tracing_subscriber::EnvFilter;

#[macro_use]
extern crate configure_me;

#[allow(clippy::all, warnings)]
mod config {
    // Ideally we would use the include_config macro from configure_me, but then we cannot use
    // #[allow(clippy::all)] to silence clippy warnings from the generated code
    include!(concat!(
        env!("OUT_DIR"),
        "/scheduler_configure_me_config.rs"
    ));
}

#[tokio::main]
async fn main() -> Result<()> {
    // parse options
    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/scheduler.toml"])
            .unwrap_or_exit();

    if opt.version {
        print_version();
        std::process::exit(0);
    }

    let special_mod_log_level = opt.log_level_setting;
    let log_dir = opt.log_dir;
    let print_thread_info = opt.print_thread_info;

    let log_file_name_prefix = format!(
        "scheduler_{}_{}_{}",
        opt.namespace, opt.external_host, opt.bind_port
    );

    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or(special_mod_log_level));
    // File layer
    if let Some(log_dir) = log_dir {
        let log_file = match opt.log_rotation_policy {
            LogRotationPolicy::Minutely => {
                tracing_appender::rolling::minutely(log_dir, &log_file_name_prefix)
            }
            LogRotationPolicy::Hourly => {
                tracing_appender::rolling::hourly(log_dir, &log_file_name_prefix)
            }
            LogRotationPolicy::Daily => {
                tracing_appender::rolling::daily(log_dir, &log_file_name_prefix)
            }
            LogRotationPolicy::Never => {
                tracing_appender::rolling::never(log_dir, &log_file_name_prefix)
            }
        };
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_thread_names(print_thread_info)
            .with_thread_ids(print_thread_info)
            .with_writer(log_file)
            .with_env_filter(log_filter)
            .init();
    } else {
        // Console layer
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_thread_names(print_thread_info)
            .with_thread_ids(print_thread_info)
            .with_writer(io::stdout)
            .with_env_filter(log_filter)
            .init();
    }

    let addr = format!("{}:{}", opt.bind_host, opt.bind_port);
    let addr = addr.parse()?;

    let cluster_storage_config = match opt.cluster_backend {
        ClusterStorage::Memory => ClusterStorageConfig::Memory,
        ClusterStorage::Etcd => ClusterStorageConfig::Etcd(
            opt.etcd_urls
                .split_whitespace()
                .map(|s| s.to_string())
                .collect(),
        ),
        ClusterStorage::Sled => {
            if opt.sled_dir.is_empty() {
                ClusterStorageConfig::Sled(None)
            } else {
                ClusterStorageConfig::Sled(Some(opt.sled_dir))
            }
        }
    };

    let task_distribution = match opt.task_distribution {
        TaskDistribution::Bias => TaskDistributionPolicy::Bias,
        TaskDistribution::RoundRobin => TaskDistributionPolicy::RoundRobin,
        TaskDistribution::ConsistentHash => {
            let num_replicas = opt.consistent_hash_num_replicas as usize;
            let tolerance = opt.consistent_hash_tolerance as usize;
            TaskDistributionPolicy::ConsistentHash {
                num_replicas,
                tolerance,
            }
        }
    };

    let config = SchedulerConfig {
        namespace: opt.namespace,
        external_host: opt.external_host,
        bind_port: opt.bind_port,
        scheduling_policy: opt.scheduler_policy,
        event_loop_buffer_size: opt.event_loop_buffer_size,
        task_distribution,
        finished_job_data_clean_up_interval_seconds: opt
            .finished_job_data_clean_up_interval_seconds,
        finished_job_state_clean_up_interval_seconds: opt
            .finished_job_state_clean_up_interval_seconds,
        advertise_flight_sql_endpoint: opt.advertise_flight_sql_endpoint,
        cluster_storage: cluster_storage_config,
        job_resubmit_interval_ms: (opt.job_resubmit_interval_ms > 0)
            .then_some(opt.job_resubmit_interval_ms),
        executor_termination_grace_period: opt.executor_termination_grace_period,
        scheduler_event_expected_processing_duration: opt
            .scheduler_event_expected_processing_duration,
        grpc_server_max_decoding_message_size: opt.grpc_server_max_decoding_message_size,
        executor_timeout_seconds: opt.executor_timeout_seconds,
        expire_dead_executor_interval_seconds: opt.expire_dead_executor_interval_seconds,
        client_auth_basic_username: opt.client_auth_basic_username,
        client_auth_basic_password: opt.client_auth_basic_password,
        client_auth_method: opt.client_auth_method,
    };

    let cluster = BallistaCluster::new_from_config(&config).await?;

    start_server(cluster, addr, Arc::new(config)).await?;
    Ok(())
}
