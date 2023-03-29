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
//

//! Ballista scheduler specific configuration

use ballista_core::config::TaskSchedulingPolicy;
use clap::ArgEnum;
use std::fmt;

/// Configurations for the ballista scheduler of scheduling jobs and tasks
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Namespace of this scheduler. Schedulers using the same cluster storage and namespace
    /// will share gloabl cluster state.
    pub namespace: String,
    /// The external hostname of the scheduler
    pub external_host: String,
    /// The bind port for the scheduler's gRPC service
    pub bind_port: u16,
    /// The task scheduling policy for the scheduler
    pub scheduling_policy: TaskSchedulingPolicy,
    /// The event loop buffer size. for a system of high throughput, a larger value like 1000000 is recommended
    pub event_loop_buffer_size: u32,
    /// The executor slots policy for the scheduler. For a cluster with single scheduler, round-robin-local is recommended
    pub executor_slots_policy: SlotsPolicy,
    /// The delayed interval for cleaning up finished job data, mainly the shuffle data, 0 means the cleaning up is disabled
    pub finished_job_data_clean_up_interval_seconds: u64,
    /// The delayed interval for cleaning up finished job state stored in the backend, 0 means the cleaning up is disabled.
    pub finished_job_state_clean_up_interval_seconds: u64,
    /// The route endpoint for proxying flight sql results via scheduler
    pub advertise_flight_sql_endpoint: Option<String>,
    /// If provided, submitted jobs which do not have tasks scheduled will be resubmitted after `job_resubmit_interval_ms`
    /// milliseconds
    pub job_resubmit_interval_ms: Option<u64>,
    /// Configuration for ballista cluster storage
    pub cluster_storage: ClusterStorageConfig,
    /// Time in seconds to allow executor for graceful shutdown. Once an executor signals it has entered Terminating status
    /// the scheduler should only consider the executor dead after this time interval has elapsed
    pub executor_termination_grace_period: u64,
    // Time in seconds before gRPC service connections are timed out
    pub grpc_client_connection_timeout: u64,
    pub grpc_server_connection_timeout: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            namespace: String::default(),
            external_host: "localhost".to_string(),
            bind_port: 50050,
            scheduling_policy: TaskSchedulingPolicy::PullStaged,
            event_loop_buffer_size: 10000,
            executor_slots_policy: SlotsPolicy::Bias,
            finished_job_data_clean_up_interval_seconds: 300,
            finished_job_state_clean_up_interval_seconds: 3600,
            advertise_flight_sql_endpoint: None,
            cluster_storage: ClusterStorageConfig::Memory,
            job_resubmit_interval_ms: None,
            executor_termination_grace_period: 0,
            grpc_client_connection_timeout: 20,
            grpc_server_connection_timeout: 20,
        }
    }
}

impl SchedulerConfig {
    pub fn scheduler_name(&self) -> String {
        format!("{}:{}", self.external_host, self.bind_port)
    }

    pub fn is_push_staged_scheduling(&self) -> bool {
        matches!(self.scheduling_policy, TaskSchedulingPolicy::PushStaged)
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.external_host = hostname.into();
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.bind_port = port;
        self
    }

    pub fn with_scheduler_policy(mut self, policy: TaskSchedulingPolicy) -> Self {
        self.scheduling_policy = policy;
        self
    }

    pub fn with_event_loop_buffer_size(mut self, buffer_size: u32) -> Self {
        self.event_loop_buffer_size = buffer_size;
        self
    }

    pub fn with_finished_job_data_clean_up_interval_seconds(
        mut self,
        interval_seconds: u64,
    ) -> Self {
        self.finished_job_data_clean_up_interval_seconds = interval_seconds;
        self
    }

    pub fn with_finished_job_state_clean_up_interval_seconds(
        mut self,
        interval_seconds: u64,
    ) -> Self {
        self.finished_job_state_clean_up_interval_seconds = interval_seconds;
        self
    }

    pub fn with_grpc_server_connection_timeout(mut self, timeout_seconds: u64) -> Self {
        self.grpc_server_connection_timeout = timeout_seconds;
        self
    }

    pub fn with_grpc_client_connection_timeout(mut self, timeout_seconds: u64) -> Self {
        self.grpc_client_connection_timeout = timeout_seconds;
        self
    }

    pub fn with_advertise_flight_sql_endpoint(
        mut self,
        endpoint: Option<String>,
    ) -> Self {
        self.advertise_flight_sql_endpoint = endpoint;
        self
    }

    pub fn with_executor_slots_policy(mut self, policy: SlotsPolicy) -> Self {
        self.executor_slots_policy = policy;
        self
    }

    pub fn with_cluster_storage(mut self, config: ClusterStorageConfig) -> Self {
        self.cluster_storage = config;
        self
    }

    pub fn with_job_resubmit_interval_ms(mut self, interval_ms: u64) -> Self {
        self.job_resubmit_interval_ms = Some(interval_ms);
        self
    }

    pub fn with_remove_executor_wait_secs(mut self, value: u64) -> Self {
        self.executor_termination_grace_period = value;
        self
    }
}

#[derive(Clone, Debug)]
pub enum ClusterStorageConfig {
    Memory,
    #[cfg(feature = "etcd")]
    Etcd(Vec<String>),
    #[cfg(feature = "sled")]
    Sled(Option<String>),
}

// an enum used to configure the executor slots policy
// needs to be visible to code generated by configure_me
#[derive(Clone, ArgEnum, Copy, Debug, serde::Deserialize)]
pub enum SlotsPolicy {
    Bias,
    RoundRobin,
    RoundRobinLocal,
}

impl SlotsPolicy {
    pub fn is_local(&self) -> bool {
        matches!(self, SlotsPolicy::RoundRobinLocal)
    }
}

impl std::str::FromStr for SlotsPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ArgEnum::from_str(s, true)
    }
}

impl parse_arg::ParseArgFromStr for SlotsPolicy {
    fn describe_type<W: fmt::Write>(mut writer: W) -> fmt::Result {
        write!(writer, "The executor slots policy for the scheduler")
    }
}
