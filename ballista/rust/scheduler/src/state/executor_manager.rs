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

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::state::backend::{Keyspace, StateBackendClient, WatchEvent};

use crate::state::{decode_into, decode_protobuf, encode_protobuf, with_lock};
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf;

use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use futures::StreamExt;
use log::{debug, info};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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

#[derive(Clone)]
pub(crate) struct ExecutorManager {
    state: Arc<dyn StateBackendClient>,
    executor_metadata: Arc<RwLock<HashMap<String, ExecutorMetadata>>>,
    executors_heartbeat: Arc<RwLock<HashMap<String, protobuf::ExecutorHeartbeat>>>,
}

impl ExecutorManager {
    pub(crate) fn new(state: Arc<dyn StateBackendClient>) -> Self {
        Self {
            state,
            executor_metadata: Arc::new(RwLock::new(HashMap::new())),
            executors_heartbeat: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize the `ExecutorManager` state. This will fill the `executor_heartbeats` value
    /// with existing heartbeats. Then new updates will be consumed through the `ExecutorHeartbeatListener`
    pub async fn init(&self) -> Result<()> {
        self.init_executor_heartbeats().await?;
        let heartbeat_listener = ExecutorHeartbeatListener::new(
            self.state.clone(),
            self.executors_heartbeat.clone(),
        );
        heartbeat_listener.start().await
    }

    /// Reserve up to n executor task slots. Once reserved these slots will not be available
    /// for scheduling.
    /// This operation is atomic, so if this method return an Err, no slots have been reserved.
    pub async fn reserve_slots(&self, n: u32) -> Result<Vec<ExecutorReservation>> {
        let lock = self.state.lock(Keyspace::Slots, "global").await?;

        with_lock(lock, async {
            debug!("Attempting to reserve {} executor slots", n);
            let start = Instant::now();
            let mut reservations: Vec<ExecutorReservation> = vec![];
            let mut desired: u32 = n;

            let alive_executors = self.get_alive_executors_within_one_minute();

            let mut txn_ops: Vec<(Keyspace, String, Vec<u8>)> = vec![];

            for executor_id in alive_executors {
                let value = self.state.get(Keyspace::Slots, &executor_id).await?;
                let mut data =
                    decode_into::<protobuf::ExecutorData, ExecutorData>(&value)?;
                let take = std::cmp::min(data.available_task_slots, desired);

                for _ in 0..take {
                    reservations.push(ExecutorReservation::new_free(executor_id.clone()));
                    data.available_task_slots -= 1;
                    desired -= 1;
                }

                let proto: protobuf::ExecutorData = data.into();
                let new_data = encode_protobuf(&proto)?;
                txn_ops.push((Keyspace::Slots, executor_id, new_data));

                if desired == 0 {
                    break;
                }
            }

            self.state.put_txn(txn_ops).await?;

            let elapsed = start.elapsed();
            info!(
                "Reserved {} executor slots in {:?}",
                reservations.len(),
                elapsed
            );

            Ok(reservations)
        })
        .await
    }

    /// Returned reserved task slots to the pool of available slots. This operation is atomic
    /// so either the entire pool of reserved task slots it returned or none are.
    pub async fn cancel_reservations(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<()> {
        let lock = self.state.lock(Keyspace::Slots, "global").await?;

        with_lock(lock, async {
            let num_reservations = reservations.len();
            debug!("Cancelling {} reservations", num_reservations);
            let start = Instant::now();

            let mut executor_slots: HashMap<String, ExecutorData> = HashMap::new();

            for reservation in reservations {
                let executor_id = &reservation.executor_id;
                if let Some(data) = executor_slots.get_mut(executor_id) {
                    data.available_task_slots += 1;
                } else {
                    let value = self.state.get(Keyspace::Slots, executor_id).await?;
                    let mut data =
                        decode_into::<protobuf::ExecutorData, ExecutorData>(&value)?;
                    data.available_task_slots += 1;
                    executor_slots.insert(executor_id.clone(), data);
                }
            }

            let txn_ops: Vec<(Keyspace, String, Vec<u8>)> = executor_slots
                .into_iter()
                .map(|(executor_id, data)| {
                    let proto: protobuf::ExecutorData = data.into();
                    let new_data = encode_protobuf(&proto)?;
                    Ok((Keyspace::Slots, executor_id, new_data))
                })
                .collect::<Result<Vec<_>>>()?;

            self.state.put_txn(txn_ops).await?;

            let elapsed = start.elapsed();
            info!(
                "Cancelled {} reservations in {:?}",
                num_reservations, elapsed
            );

            Ok(())
        })
        .await
    }

    /// Get a list of all executors along with the timestamp of their last recorded heartbeat
    pub async fn get_executor_state(&self) -> Result<Vec<(ExecutorMetadata, Duration)>> {
        let heartbeat_timestamps: Vec<(String, u64)> = {
            let heartbeats = self.executors_heartbeat.read();

            heartbeats
                .iter()
                .map(|(executor_id, heartbeat)| {
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
            let metadata_cache = self.executor_metadata.read();
            if let Some(cached) = metadata_cache.get(executor_id) {
                return Ok(cached.clone());
            }
        }

        let value = self.state.get(Keyspace::Executors, executor_id).await?;

        let decoded =
            decode_into::<protobuf::ExecutorMetadata, ExecutorMetadata>(&value)?;
        Ok(decoded)
    }

    pub async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        let executor_id = metadata.id.clone();
        let proto: protobuf::ExecutorMetadata = metadata.into();
        let value = encode_protobuf(&proto)?;

        self.state
            .put(Keyspace::Executors, executor_id, value)
            .await
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
        self.test_scheduler_connectivity(&metadata).await?;

        let executor_id = metadata.id.clone();

        let current_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Error getting current timestamp: {:?}",
                    e
                ))
            })?
            .as_secs();

        //TODO this should be in a transaction
        // Now that we know we can connect, save the metadata and slots
        self.save_executor_metadata(metadata).await?;
        self.save_executor_heartbeat(protobuf::ExecutorHeartbeat {
            executor_id: executor_id.clone(),
            timestamp: current_ts,
            state: None,
        })
        .await?;

        if !reserve {
            let proto: protobuf::ExecutorData = specification.into();
            let value = encode_protobuf(&proto)?;
            self.state.put(Keyspace::Slots, executor_id, value).await?;
            Ok(vec![])
        } else {
            let mut specification = specification;
            let num_slots = specification.available_task_slots as usize;
            let mut reservations: Vec<ExecutorReservation> = vec![];
            for _ in 0..num_slots {
                reservations.push(ExecutorReservation::new_free(executor_id.clone()));
            }

            specification.available_task_slots = 0;
            let proto: protobuf::ExecutorData = specification.into();
            let value = encode_protobuf(&proto)?;
            self.state.put(Keyspace::Slots, executor_id, value).await?;
            Ok(reservations)
        }
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
        heartbeat: protobuf::ExecutorHeartbeat,
    ) -> Result<()> {
        let executor_id = heartbeat.executor_id.clone();
        let value = encode_protobuf(&heartbeat)?;
        self.state
            .put(Keyspace::Heartbeats, executor_id, value)
            .await?;

        let mut executors_heartbeat = self.executors_heartbeat.write();
        executors_heartbeat.insert(heartbeat.executor_id.clone(), heartbeat);

        Ok(())
    }

    /// Initialize the set of executor heartbeats from storage
    pub(crate) async fn init_executor_heartbeats(&self) -> Result<()> {
        let heartbeats = self.state.scan(Keyspace::Heartbeats, None).await?;
        let mut cache = self.executors_heartbeat.write();

        for (_, value) in heartbeats {
            let data: protobuf::ExecutorHeartbeat = decode_protobuf(&value)?;
            let executor_id = data.executor_id.clone();
            cache.insert(executor_id, data);
        }
        Ok(())
    }

    /// Retrieve the set of all executor IDs where the executor has been observed in the last
    /// `last_seen_ts_threshold` seconds.
    pub(crate) fn get_alive_executors(
        &self,
        last_seen_ts_threshold: u64,
    ) -> HashSet<String> {
        let executors_heartbeat = self.executors_heartbeat.read();
        executors_heartbeat
            .iter()
            .filter_map(|(exec, heartbeat)| {
                (heartbeat.timestamp > last_seen_ts_threshold).then(|| exec.clone())
            })
            .collect()
    }

    #[allow(dead_code)]
    fn get_alive_executors_within_one_minute(&self) -> HashSet<String> {
        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let last_seen_threshold = now_epoch_ts
            .checked_sub(Duration::from_secs(60))
            .unwrap_or_else(|| Duration::from_secs(0));
        self.get_alive_executors(last_seen_threshold.as_secs())
    }
}

/// Rather than doing a scan across persistent state to find alive executors every time
/// we need to find the set of alive executors, we start a watch on the `Heartbeats` keyspace
/// and maintain an in-memory copy of the executor heartbeats.
struct ExecutorHeartbeatListener {
    state: Arc<dyn StateBackendClient>,
    executors_heartbeat: Arc<RwLock<HashMap<String, protobuf::ExecutorHeartbeat>>>,
}

impl ExecutorHeartbeatListener {
    pub fn new(
        state: Arc<dyn StateBackendClient>,
        executors_heartbeat: Arc<RwLock<HashMap<String, protobuf::ExecutorHeartbeat>>>,
    ) -> Self {
        Self {
            state,
            executors_heartbeat,
        }
    }

    /// Spawn an sync task which will watch the the Heartbeats keyspace and insert
    /// new heartbeats in the `executors_heartbeat` cache.
    pub async fn start(&self) -> Result<()> {
        let mut watch = self
            .state
            .watch(Keyspace::Heartbeats, "".to_owned())
            .await?;
        let heartbeats = self.executors_heartbeat.clone();
        tokio::task::spawn(async move {
            while let Some(event) = watch.next().await {
                if let WatchEvent::Put(_, value) = event {
                    if let Ok(data) =
                        decode_protobuf::<protobuf::ExecutorHeartbeat>(&value)
                    {
                        let executor_id = data.executor_id.clone();
                        let mut heartbeats = heartbeats.write();

                        heartbeats.insert(executor_id, data);
                    }
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::state::backend::standalone::StandaloneClient;
    use crate::state::executor_manager::{ExecutorManager, ExecutorReservation};
    use ballista_core::error::Result;
    use ballista_core::serde::scheduler::{
        ExecutorData, ExecutorMetadata, ExecutorSpecification,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_reserve_and_cancel() -> Result<()> {
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);

        let executor_manager = ExecutorManager::new(state_storage);

        let executors = test_executors(10, 4);

        for (executor_metadata, executor_data) in executors {
            executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        // Reserve all the slots
        let reservations = executor_manager.reserve_slots(40).await?;

        assert_eq!(reservations.len(), 40);

        // Now cancel them
        executor_manager.cancel_reservations(reservations).await?;

        // Now reserve again
        let reservations = executor_manager.reserve_slots(40).await?;

        assert_eq!(reservations.len(), 40);

        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_partial() -> Result<()> {
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);

        let executor_manager = ExecutorManager::new(state_storage);

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
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<Result<Vec<ExecutorReservation>>>(1000);

        let executors = test_executors(10, 4);

        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);

        let executor_manager = ExecutorManager::new(state_storage);

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
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);

        let executor_manager = ExecutorManager::new(state_storage);

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

    fn test_executors(
        total_executors: usize,
        slots_per_executor: u32,
    ) -> Vec<(ExecutorMetadata, ExecutorData)> {
        let mut result: Vec<(ExecutorMetadata, ExecutorData)> = vec![];

        for i in 0..total_executors {
            result.push((
                ExecutorMetadata {
                    id: format!("executor-{}", i),
                    host: format!("host-{}", i),
                    port: 8080,
                    grpc_port: 9090,
                    specification: ExecutorSpecification {
                        task_slots: slots_per_executor,
                    },
                },
                ExecutorData {
                    executor_id: format!("executor-{}", i),
                    total_task_slots: slots_per_executor,
                    available_task_slots: slots_per_executor,
                },
            ));
        }

        result
    }
}
