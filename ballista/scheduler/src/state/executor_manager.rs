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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cluster::TaskDistribution;

use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf;

use crate::cluster::ClusterState;
use crate::config::SlotsPolicy;

use crate::state::execution_graph::RunningTaskInfo;
use ballista_core::serde::protobuf::executor_grpc_client::ExecutorGrpcClient;
use ballista_core::serde::protobuf::{
    executor_status, CancelTasksParams, ExecutorHeartbeat, ExecutorStatus,
    RemoveJobDataParams,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use ballista_core::utils::create_grpc_client_connection;
use dashmap::{DashMap, DashSet};
use futures::StreamExt;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tonic::transport::Channel;

type ExecutorClients = Arc<DashMap<String, ExecutorGrpcClient<Channel>>>;

/// Represents a task slot that is reserved (i.e. available for scheduling but not visible to the
/// rest of the system).
/// When tasks finish we want to preferentially assign new tasks from the same job, so the reservation
/// can already be assigned to a particular job ID. In that case, the scheduler will try to schedule
/// available tasks for that job to the reserved task slot.
#[derive(Clone, Debug)]
pub struct ExecutorReservation {
    pub executor_id: String,
    pub job_id: Option<String>,
}

impl ExecutorReservation {
    pub fn new_free(executor_id: String) -> Self {
        Self {
            executor_id,
            job_id: None,
        }
    }

    pub fn new_assigned(executor_id: String, job_id: String) -> Self {
        Self {
            executor_id,
            job_id: Some(job_id),
        }
    }

    pub fn assign(mut self, job_id: String) -> Self {
        self.job_id = Some(job_id);
        self
    }

    pub fn assigned(&self) -> bool {
        self.job_id.is_some()
    }
}

// TODO move to configuration file
/// Default executor timeout in seconds, it should be longer than executor's heartbeat intervals.
/// Only after missing two or tree consecutive heartbeats from a executor, the executor is mark
/// to be dead.
pub const DEFAULT_EXECUTOR_TIMEOUT_SECONDS: u64 = 180;

// TODO move to configuration file
/// Interval check for expired or dead executors
pub const EXPIRE_DEAD_EXECUTOR_INTERVAL_SECS: u64 = 15;

#[derive(Clone)]
pub struct ExecutorManager {
    // executor slot policy
    slots_policy: SlotsPolicy,
    task_distribution: TaskDistribution,
    cluster_state: Arc<dyn ClusterState>,
    // executor_id -> ExecutorMetadata map
    executor_metadata: Arc<DashMap<String, ExecutorMetadata>>,
    // executor_id -> ExecutorHeartbeat map
    executors_heartbeat: Arc<DashMap<String, protobuf::ExecutorHeartbeat>>,
    // executor_id -> ExecutorData map, only used when the slots policy is of local
    executor_data: Arc<Mutex<HashMap<String, ExecutorData>>>,
    // dead executor sets:
    dead_executors: Arc<DashSet<String>>,
    clients: ExecutorClients,
    grpc_client_connection_timeout: Duration,
}

impl ExecutorManager {
    pub(crate) fn new(
        cluster_state: Arc<dyn ClusterState>,
        slots_policy: SlotsPolicy,
        timeout: Duration,
    ) -> Self {
        let task_distribution = match slots_policy {
            SlotsPolicy::Bias => TaskDistribution::Bias,
            SlotsPolicy::RoundRobin | SlotsPolicy::RoundRobinLocal => {
                TaskDistribution::RoundRobin
            }
        };

        Self {
            slots_policy,
            task_distribution,
            cluster_state,
            executor_metadata: Arc::new(DashMap::new()),
            executors_heartbeat: Arc::new(DashMap::new()),
            executor_data: Arc::new(Mutex::new(HashMap::new())),
            dead_executors: Arc::new(DashSet::new()),
            clients: Default::default(),
            grpc_client_connection_timeout: timeout,
        }
    }

    /// Initialize a background process that will listen for executor heartbeats and update the in-memory cache
    /// of executor heartbeats
    pub async fn init(&self) -> Result<()> {
        self.init_active_executor_heartbeats().await?;

        let mut heartbeat_stream = self.cluster_state.executor_heartbeat_stream().await?;

        info!("Initializing heartbeat listener");

        let heartbeats = self.executors_heartbeat.clone();
        let dead_executors = self.dead_executors.clone();
        tokio::task::spawn(async move {
            while let Some(heartbeat) = heartbeat_stream.next().await {
                let executor_id = heartbeat.executor_id.clone();

                match heartbeat
                    .status
                    .as_ref()
                    .and_then(|status| status.status.as_ref())
                {
                    Some(executor_status::Status::Dead(_)) => {
                        heartbeats.remove(&executor_id);
                        dead_executors.insert(executor_id);
                    }
                    _ => {
                        heartbeats.insert(executor_id, heartbeat);
                    }
                }
            }
        });

        Ok(())
    }

    /// Reserve up to n executor task slots. Once reserved these slots will not be available
    /// for scheduling.
    /// This operation is atomic, so if this method return an Err, no slots have been reserved.
    pub async fn reserve_slots(&self, n: u32) -> Result<Vec<ExecutorReservation>> {
        if self.slots_policy.is_local() {
            self.reserve_slots_local(n).await
        } else {
            let alive_executors = self.get_alive_executors_within_one_minute();

            debug!("Alive executors: {alive_executors:?}");

            self.cluster_state
                .reserve_slots(n, self.task_distribution, Some(alive_executors))
                .await
        }
    }

    async fn reserve_slots_local(&self, n: u32) -> Result<Vec<ExecutorReservation>> {
        debug!("Attempting to reserve {} executor slots", n);

        let alive_executors = self.get_alive_executors_within_one_minute();

        match self.slots_policy {
            SlotsPolicy::RoundRobinLocal => {
                self.reserve_slots_local_round_robin(n, alive_executors)
                    .await
            }
            _ => Err(BallistaError::General(format!(
                "Reservation policy {:?} is not supported",
                self.slots_policy
            ))),
        }
    }

    /// Create ExecutorReservation in a round robin way to evenly assign tasks to executors
    async fn reserve_slots_local_round_robin(
        &self,
        mut n: u32,
        alive_executors: HashSet<String>,
    ) -> Result<Vec<ExecutorReservation>> {
        let mut executor_data = self.executor_data.lock();

        let mut available_executor_data: Vec<&mut ExecutorData> = executor_data
            .values_mut()
            .filter_map(|data| {
                (data.available_task_slots > 0
                    && alive_executors.contains(&data.executor_id))
                .then_some(data)
            })
            .collect();
        available_executor_data
            .sort_by(|a, b| Ord::cmp(&b.available_task_slots, &a.available_task_slots));

        let mut reservations: Vec<ExecutorReservation> = vec![];

        // Exclusive
        let mut last_updated_idx = 0usize;
        loop {
            let n_before = n;
            for (idx, data) in available_executor_data.iter_mut().enumerate() {
                if n == 0 {
                    break;
                }

                // Since the vector is sorted in descending order,
                // if finding one executor has not enough slots, the following will have not enough, either
                if data.available_task_slots == 0 {
                    break;
                }

                reservations
                    .push(ExecutorReservation::new_free(data.executor_id.clone()));
                data.available_task_slots -= 1;
                n -= 1;

                if idx >= last_updated_idx {
                    last_updated_idx = idx + 1;
                }
            }

            if n_before == n {
                break;
            }
        }

        Ok(reservations)
    }

    /// Returned reserved task slots to the pool of available slots. This operation is atomic
    /// so either the entire pool of reserved task slots it returned or none are.
    pub async fn cancel_reservations(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<()> {
        if self.slots_policy.is_local() {
            self.cancel_reservations_local(reservations).await
        } else {
            self.cluster_state.cancel_reservations(reservations).await
        }
    }

    async fn cancel_reservations_local(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<()> {
        let mut executor_slots: HashMap<String, u32> = HashMap::new();
        for reservation in reservations {
            if let Some(slots) = executor_slots.get_mut(&reservation.executor_id) {
                *slots += 1;
            } else {
                executor_slots.insert(reservation.executor_id, 1);
            }
        }

        let mut executor_data = self.executor_data.lock();
        for (id, released_slots) in executor_slots.into_iter() {
            if let Some(slots) = executor_data.get_mut(&id) {
                slots.available_task_slots += released_slots;
            } else {
                warn!("ExecutorData for {} is not cached in memory", id);
            }
        }

        Ok(())
    }

    /// Send rpc to Executors to cancel the running tasks
    pub async fn cancel_running_tasks(&self, tasks: Vec<RunningTaskInfo>) -> Result<()> {
        let mut tasks_to_cancel: HashMap<&str, Vec<protobuf::RunningTaskInfo>> =
            Default::default();

        for task_info in &tasks {
            if let Some(infos) = tasks_to_cancel.get_mut(task_info.executor_id.as_str()) {
                infos.push(protobuf::RunningTaskInfo {
                    task_id: task_info.task_id as u32,
                    job_id: task_info.job_id.clone(),
                    stage_id: task_info.stage_id as u32,
                    partition_id: task_info.partition_id as u32,
                })
            } else {
                tasks_to_cancel.insert(
                    task_info.executor_id.as_str(),
                    vec![protobuf::RunningTaskInfo {
                        task_id: task_info.task_id as u32,
                        job_id: task_info.job_id.clone(),
                        stage_id: task_info.stage_id as u32,
                        partition_id: task_info.partition_id as u32,
                    }],
                );
            }
        }

        for (executor_id, infos) in tasks_to_cancel {
            if let Ok(mut client) = self.get_client(executor_id).await {
                client
                    .cancel_tasks(CancelTasksParams { task_infos: infos })
                    .await?;
            } else {
                error!(
                    "Failed to get client for executor ID {} to cancel tasks",
                    executor_id
                )
            }
        }
        Ok(())
    }

    /// Send rpc to Executors to clean up the job data by delayed clean_up_interval seconds
    pub(crate) fn clean_up_job_data_delayed(
        &self,
        job_id: String,
        clean_up_interval: u64,
    ) {
        if clean_up_interval == 0 {
            info!(
                "The interval is 0 and the clean up for job data {} will not triggered",
                job_id
            );
            return;
        }

        let executor_manager = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(clean_up_interval)).await;
            executor_manager.clean_up_job_data_inner(job_id).await;
        });
    }

    /// Send rpc to Executors to clean up the job data in a spawn thread
    pub fn clean_up_job_data(&self, job_id: String) {
        let executor_manager = self.clone();
        tokio::spawn(async move {
            executor_manager.clean_up_job_data_inner(job_id).await;
        });
    }

    /// Send rpc to Executors to clean up the job data
    async fn clean_up_job_data_inner(&self, job_id: String) {
        let alive_executors = self.get_alive_executors_within_one_minute();
        for executor in alive_executors {
            let job_id_clone = job_id.to_owned();
            if let Ok(mut client) = self.get_client(&executor).await {
                tokio::spawn(async move {
                    if let Err(err) = client
                        .remove_job_data(RemoveJobDataParams {
                            job_id: job_id_clone,
                        })
                        .await
                    {
                        warn!(
                            "Failed to call remove_job_data on Executor {} due to {:?}",
                            executor, err
                        )
                    }
                });
            } else {
                warn!("Failed to get client for Executor {}", executor)
            }
        }
    }

    pub async fn get_client(
        &self,
        executor_id: &str,
    ) -> Result<ExecutorGrpcClient<Channel>> {
        let client = self.clients.get(executor_id).map(|value| value.clone());

        if let Some(client) = client {
            Ok(client)
        } else {
            let executor_metadata = self.get_executor_metadata(executor_id).await?;
            let executor_url = format!(
                "http://{}:{}",
                executor_metadata.host, executor_metadata.grpc_port
            );
            let connection = create_grpc_client_connection(
                executor_url,
                self.grpc_client_connection_timeout,
            )
            .await?;
            let client = ExecutorGrpcClient::new(connection);

            {
                self.clients.insert(executor_id.to_owned(), client.clone());
            }
            Ok(client)
        }
    }

    /// Get a list of all executors along with the timestamp of their last recorded heartbeat
    pub async fn get_executor_state(&self) -> Result<Vec<(ExecutorMetadata, Duration)>> {
        let heartbeat_timestamps: Vec<(String, u64)> = {
            self.executors_heartbeat
                .iter()
                .map(|item| {
                    let (executor_id, heartbeat) = item.pair();
                    (executor_id.clone(), heartbeat.timestamp)
                })
                .collect()
        };

        let mut state: Vec<(ExecutorMetadata, Duration)> = vec![];
        for (executor_id, ts) in heartbeat_timestamps {
            let duration = Duration::from_secs(ts);

            let metadata = self.get_executor_metadata(&executor_id).await?;

            state.push((metadata, duration));
        }

        Ok(state)
    }

    pub async fn get_executor_metadata(
        &self,
        executor_id: &str,
    ) -> Result<ExecutorMetadata> {
        {
            if let Some(cached) = self.executor_metadata.get(executor_id) {
                return Ok(cached.clone());
            }
        }

        self.cluster_state.get_executor_metadata(executor_id).await
    }

    pub async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        self.cluster_state.save_executor_metadata(metadata).await
    }

    /// Register the executor with the scheduler. This will save the executor metadata and the
    /// executor data to persistent state.
    ///
    /// If `reserve` is true, then any available task slots will be reserved and dispatched for scheduling.
    /// If `reserve` is false, then the executor data will be saved as is.
    ///
    /// In general, reserve should be true is the scheduler is using push-based scheduling and false
    /// if the scheduler is using pull-based scheduling.
    pub async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        specification: ExecutorData,
        reserve: bool,
    ) -> Result<Vec<ExecutorReservation>> {
        debug!(
            "registering executor {} with {} task slots",
            metadata.id, specification.total_task_slots
        );

        self.test_scheduler_connectivity(&metadata).await?;

        let current_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                BallistaError::Internal(format!("Error getting current timestamp: {e:?}"))
            })?
            .as_secs();

        let initial_heartbeat = ExecutorHeartbeat {
            executor_id: metadata.id.clone(),
            timestamp: current_ts,
            metrics: vec![],
            status: Some(ExecutorStatus {
                status: Some(executor_status::Status::Active(String::default())),
            }),
        };

        if !reserve {
            if self.slots_policy.is_local() {
                let mut executor_data = self.executor_data.lock();
                executor_data
                    .insert(specification.executor_id.clone(), specification.clone());
            }

            self.cluster_state
                .register_executor(metadata, specification.clone(), reserve)
                .await?;

            self.executors_heartbeat
                .insert(initial_heartbeat.executor_id.clone(), initial_heartbeat);

            Ok(vec![])
        } else {
            let mut specification = specification;
            let num_slots = specification.available_task_slots as usize;
            let mut reservations: Vec<ExecutorReservation> = vec![];
            for _ in 0..num_slots {
                reservations.push(ExecutorReservation::new_free(metadata.id.clone()));
            }

            specification.available_task_slots = 0;

            if self.slots_policy.is_local() {
                let mut executor_data = self.executor_data.lock();
                executor_data
                    .insert(specification.executor_id.clone(), specification.clone());
            }

            self.cluster_state
                .register_executor(metadata, specification, reserve)
                .await?;

            self.executors_heartbeat
                .insert(initial_heartbeat.executor_id.clone(), initial_heartbeat);

            Ok(reservations)
        }
    }

    /// Remove the executor within the scheduler.
    pub async fn remove_executor(
        &self,
        executor_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        info!("Removing executor {}: {:?}", executor_id, reason);
        self.cluster_state.remove_executor(executor_id).await?;

        let executor_id = executor_id.to_owned();

        self.executors_heartbeat.remove(&executor_id);

        // Remove executor data cache for dead executors
        {
            let mut executor_data = self.executor_data.lock();
            executor_data.remove(&executor_id);
        }

        self.dead_executors.insert(executor_id);

        Ok(())
    }

    #[cfg(not(test))]
    async fn test_scheduler_connectivity(
        &self,
        metadata: &ExecutorMetadata,
    ) -> Result<()> {
        let executor_url = format!("http://{}:{}", metadata.host, metadata.grpc_port);
        debug!("Connecting to executor {:?}", executor_url);
        let _ = protobuf::executor_grpc_client::ExecutorGrpcClient::connect(executor_url)
            .await
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Failed to register executor at {}:{}, could not connect: {:?}",
                    metadata.host, metadata.grpc_port, e
                ))
            })?;
        Ok(())
    }

    #[cfg(test)]
    async fn test_scheduler_connectivity(
        &self,
        _metadata: &ExecutorMetadata,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn save_executor_heartbeat(
        &self,
        heartbeat: ExecutorHeartbeat,
    ) -> Result<()> {
        self.cluster_state
            .save_executor_heartbeat(heartbeat.clone())
            .await?;

        self.executors_heartbeat
            .insert(heartbeat.executor_id.clone(), heartbeat);

        Ok(())
    }

    pub(crate) fn is_dead_executor(&self, executor_id: &str) -> bool {
        self.dead_executors.contains(executor_id)
    }

    /// Initialize the set of active executor heartbeats from storage
    async fn init_active_executor_heartbeats(&self) -> Result<()> {
        let heartbeats = self.cluster_state.executor_heartbeats().await?;

        for (executor_id, heartbeat) in heartbeats {
            // let data: protobuf::ExecutorHeartbeat = decode_protobuf(&value)?;
            if let Some(ExecutorStatus {
                status: Some(executor_status::Status::Active(_)),
            }) = heartbeat.status
            {
                self.executors_heartbeat.insert(executor_id, heartbeat);
            }
        }
        Ok(())
    }

    /// Retrieve the set of all executor IDs where the executor has been observed in the last
    /// `last_seen_ts_threshold` seconds.
    pub(crate) fn get_alive_executors(
        &self,
        last_seen_ts_threshold: u64,
    ) -> HashSet<String> {
        self.executors_heartbeat
            .iter()
            .filter_map(|pair| {
                let (exec, heartbeat) = pair.pair();

                let active = matches!(
                    heartbeat
                        .status
                        .as_ref()
                        .and_then(|status| status.status.as_ref()),
                    Some(executor_status::Status::Active(_))
                );
                let live = heartbeat.timestamp > last_seen_ts_threshold;

                (active && live).then(|| exec.clone())
            })
            .collect()
    }

    /// Return a list of expired executors
    pub(crate) fn get_expired_executors(
        &self,
        termination_grace_period: u64,
    ) -> Vec<ExecutorHeartbeat> {
        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        // Threshold for last heartbeat from Active executor before marking dead
        let last_seen_threshold = now_epoch_ts
            .checked_sub(Duration::from_secs(DEFAULT_EXECUTOR_TIMEOUT_SECONDS))
            .unwrap_or_else(|| Duration::from_secs(0))
            .as_secs();

        // Threshold for last heartbeat for Fenced executor before marking dead
        let termination_wait_threshold = now_epoch_ts
            .checked_sub(Duration::from_secs(termination_grace_period))
            .unwrap_or_else(|| Duration::from_secs(0))
            .as_secs();

        let expired_executors = self
            .executors_heartbeat
            .iter()
            .filter_map(|pair| {
                let (_exec, heartbeat) = pair.pair();

                let terminating = matches!(
                    heartbeat
                        .status
                        .as_ref()
                        .and_then(|status| status.status.as_ref()),
                    Some(executor_status::Status::Terminating(_))
                );

                let grace_period_expired =
                    heartbeat.timestamp <= termination_wait_threshold;

                let expired = heartbeat.timestamp <= last_seen_threshold;

                ((terminating && grace_period_expired) || expired)
                    .then(|| heartbeat.clone())
            })
            .collect::<Vec<_>>();
        expired_executors
    }

    pub(crate) fn get_alive_executors_within_one_minute(&self) -> HashSet<String> {
        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let last_seen_threshold = now_epoch_ts
            .checked_sub(Duration::from_secs(60))
            .unwrap_or_else(|| Duration::from_secs(0));
        self.get_alive_executors(last_seen_threshold.as_secs())
    }
}

#[cfg(test)]
mod test {

    use std::time::Duration;

    use crate::config::SlotsPolicy;

    use crate::scheduler_server::timestamp_secs;
    use crate::state::executor_manager::{ExecutorManager, ExecutorReservation};
    use crate::test_utils::test_cluster_context;
    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::executor_status::Status;
    use ballista_core::serde::protobuf::{ExecutorHeartbeat, ExecutorStatus};
    use ballista_core::serde::scheduler::{
        ExecutorData, ExecutorMetadata, ExecutorSpecification,
    };

    #[tokio::test]
    async fn test_reserve_and_cancel() -> Result<()> {
        test_reserve_and_cancel_inner(SlotsPolicy::Bias).await?;
        test_reserve_and_cancel_inner(SlotsPolicy::RoundRobin).await?;
        test_reserve_and_cancel_inner(SlotsPolicy::RoundRobinLocal).await?;

        Ok(())
    }

    async fn test_reserve_and_cancel_inner(slots_policy: SlotsPolicy) -> Result<()> {
        let cluster = test_cluster_context();

        let executor_manager = ExecutorManager::new(
            cluster.cluster_state(),
            slots_policy,
            Duration::from_secs(20),
        );

        let executors = test_executors(10, 4);

        for (executor_metadata, executor_data) in executors {
            executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        // Reserve all the slots
        let reservations = executor_manager.reserve_slots(40).await?;

        assert_eq!(
            reservations.len(),
            40,
            "Expected 40 reservations for policy {slots_policy:?}"
        );

        // Now cancel them
        executor_manager.cancel_reservations(reservations).await?;

        // Now reserve again
        let reservations = executor_manager.reserve_slots(40).await?;

        assert_eq!(
            reservations.len(),
            40,
            "Expected 40 reservations for policy {slots_policy:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_partial() -> Result<()> {
        test_reserve_partial_inner(SlotsPolicy::Bias).await?;
        test_reserve_partial_inner(SlotsPolicy::RoundRobin).await?;
        test_reserve_partial_inner(SlotsPolicy::RoundRobinLocal).await?;

        Ok(())
    }

    async fn test_reserve_partial_inner(slots_policy: SlotsPolicy) -> Result<()> {
        let cluster = test_cluster_context();

        let executor_manager = ExecutorManager::new(
            cluster.cluster_state(),
            slots_policy,
            Duration::from_secs(20),
        );

        let executors = test_executors(10, 4);

        for (executor_metadata, executor_data) in executors {
            executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        // Reserve all the slots
        let reservations = executor_manager.reserve_slots(30).await?;

        assert_eq!(reservations.len(), 30);

        // Try to reserve 30 more. Only ten are available though so we should only get 10
        let more_reservations = executor_manager.reserve_slots(30).await?;

        assert_eq!(more_reservations.len(), 10);

        // Now cancel them
        executor_manager.cancel_reservations(reservations).await?;
        executor_manager
            .cancel_reservations(more_reservations)
            .await?;

        // Now reserve again
        let reservations = executor_manager.reserve_slots(40).await?;

        assert_eq!(reservations.len(), 40);

        let more_reservations = executor_manager.reserve_slots(30).await?;

        assert_eq!(more_reservations.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_concurrent() -> Result<()> {
        test_reserve_concurrent_inner(SlotsPolicy::Bias).await?;
        test_reserve_concurrent_inner(SlotsPolicy::RoundRobin).await?;
        test_reserve_concurrent_inner(SlotsPolicy::RoundRobinLocal).await?;

        Ok(())
    }

    async fn test_reserve_concurrent_inner(slots_policy: SlotsPolicy) -> Result<()> {
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<Result<Vec<ExecutorReservation>>>(1000);

        let executors = test_executors(10, 4);

        let cluster = test_cluster_context();
        let executor_manager = ExecutorManager::new(
            cluster.cluster_state(),
            slots_policy,
            Duration::from_secs(20),
        );

        for (executor_metadata, executor_data) in executors {
            executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        {
            let sender = sender;
            // Spawn 20 async tasks to each try and reserve all 40 slots
            for _ in 0..20 {
                let executor_manager = executor_manager.clone();
                let sender = sender.clone();
                tokio::task::spawn(async move {
                    let reservations = executor_manager.reserve_slots(40).await;
                    sender.send(reservations).await.unwrap();
                });
            }
        }

        let mut total_reservations: Vec<ExecutorReservation> = vec![];

        while let Some(Ok(reservations)) = receiver.recv().await {
            total_reservations.extend(reservations);
        }

        // The total number of reservations should never exceed the number of slots
        assert_eq!(total_reservations.len(), 40);

        Ok(())
    }

    #[tokio::test]
    async fn test_register_reserve() -> Result<()> {
        test_register_reserve_inner(SlotsPolicy::Bias).await?;
        test_register_reserve_inner(SlotsPolicy::RoundRobin).await?;
        test_register_reserve_inner(SlotsPolicy::RoundRobinLocal).await?;

        Ok(())
    }

    async fn test_register_reserve_inner(slots_policy: SlotsPolicy) -> Result<()> {
        let cluster = test_cluster_context();

        let executor_manager = ExecutorManager::new(
            cluster.cluster_state(),
            slots_policy,
            Duration::from_secs(20),
        );

        let executors = test_executors(10, 4);

        for (executor_metadata, executor_data) in executors {
            let reservations = executor_manager
                .register_executor(executor_metadata, executor_data, true)
                .await?;

            assert_eq!(reservations.len(), 4);
        }

        // All slots should be reserved
        let reservations = executor_manager.reserve_slots(1).await?;

        assert_eq!(reservations.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_ignore_fenced_executors() -> Result<()> {
        test_ignore_fenced_executors_inner(SlotsPolicy::Bias).await?;
        test_ignore_fenced_executors_inner(SlotsPolicy::RoundRobin).await?;
        test_ignore_fenced_executors_inner(SlotsPolicy::RoundRobinLocal).await?;

        Ok(())
    }

    async fn test_ignore_fenced_executors_inner(slots_policy: SlotsPolicy) -> Result<()> {
        let cluster = test_cluster_context();

        let executor_manager = ExecutorManager::new(
            cluster.cluster_state(),
            slots_policy,
            Duration::from_secs(20),
        );

        // Setup two executors initially
        let executors = test_executors(2, 4);

        for (executor_metadata, executor_data) in executors {
            let _ = executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        // Fence one of the executors
        executor_manager
            .save_executor_heartbeat(ExecutorHeartbeat {
                executor_id: "executor-0".to_string(),
                timestamp: timestamp_secs(),
                metrics: vec![],
                status: Some(ExecutorStatus {
                    status: Some(Status::Terminating(String::default())),
                }),
            })
            .await?;

        let reservations = executor_manager.reserve_slots(8).await?;

        assert_eq!(reservations.len(), 4, "Expected only four reservations");

        assert!(
            reservations
                .iter()
                .all(|res| res.executor_id == "executor-1"),
            "Expected all reservations from non-fenced executor",
        );

        Ok(())
    }

    fn test_executors(
        total_executors: usize,
        slots_per_executor: u32,
    ) -> Vec<(ExecutorMetadata, ExecutorData)> {
        let mut result: Vec<(ExecutorMetadata, ExecutorData)> = vec![];

        for i in 0..total_executors {
            result.push((
                ExecutorMetadata {
                    id: format!("executor-{i}"),
                    host: format!("host-{i}"),
                    port: 8080,
                    grpc_port: 9090,
                    specification: ExecutorSpecification {
                        task_slots: slots_per_executor,
                    },
                },
                ExecutorData {
                    executor_id: format!("executor-{i}"),
                    total_task_slots: slots_per_executor,
                    available_task_slots: slots_per_executor,
                },
            ));
        }

        result
    }
}
