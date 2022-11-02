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
    /// The route endpoint for proxying flight results via scheduler
    pub advertise_flight_result_route_endpoint: Option<String>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            scheduling_policy: TaskSchedulingPolicy::PullStaged,
            event_loop_buffer_size: 10000,
            executor_slots_policy: SlotsPolicy::Bias,
            finished_job_data_clean_up_interval_seconds: 300,
            finished_job_state_clean_up_interval_seconds: 3600,
            advertise_flight_result_route_endpoint: None,
        }
    }
}

impl SchedulerConfig {
    pub fn is_push_staged_scheduling(&self) -> bool {
        matches!(self.scheduling_policy, TaskSchedulingPolicy::PushStaged)
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

    pub fn with_advertise_flight_result_route_endpoint(
        mut self,
        endpoint: Option<String>,
    ) -> Self {
        self.advertise_flight_result_route_endpoint = endpoint;
        self
    }
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
