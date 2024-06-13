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

use crate::cluster::storage::{KeyValueStore, Keyspace, Lock, Operation, WatchEvent};
use crate::cluster::{
    bind_task_bias, bind_task_consistent_hash, bind_task_round_robin, get_scan_files,
    is_skip_consistent_hash, BoundTask, ClusterState, ExecutorHeartbeatStream,
    ExecutorSlot, JobState, JobStateEvent, JobStateEventStream, JobStatus,
    TaskDistributionPolicy, TopologyNode,
};
use crate::scheduler_server::{timestamp_secs, SessionBuilder};
use crate::state::execution_graph::ExecutionGraph;
use crate::state::session_manager::create_datafusion_context;
use crate::state::task_manager::JobInfoCache;
use crate::state::{decode_into, decode_protobuf};
use async_trait::async_trait;
use ballista_core::config::BallistaConfig;
use ballista_core::consistent_hash::node::Node;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{
    self, AvailableTaskSlots, ExecutorHeartbeat, ExecutorTaskSlots, FailedJob,
    KeyValuePair, QueuedJob,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use ballista_core::serde::BallistaCodec;
use dashmap::DashMap;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::StreamExt;
use itertools::Itertools;
use log::{error, info, warn};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

/// State implementation based on underlying `KeyValueStore`
pub struct KeyValueState<
    S: KeyValueStore,
    T: 'static + AsLogicalPlan = LogicalPlanNode,
    U: 'static + AsExecutionPlan = PhysicalPlanNode,
> {
    /// Underlying `KeyValueStore`
    store: S,
    /// ExecutorMetadata cache, executor_id -> ExecutorMetadata
    executors: Arc<DashMap<String, ExecutorMetadata>>,
    /// ExecutorHeartbeat cache, executor_id -> ExecutorHeartbeat
    executor_heartbeats: Arc<DashMap<String, ExecutorHeartbeat>>,
    /// Codec used to serialize/deserialize execution plan
    codec: BallistaCodec<T, U>,
    /// Name of current scheduler. Should be `{host}:{port}`
    #[allow(dead_code)]
    scheduler: String,
    /// In-memory store of queued jobs. Map from Job ID -> (Job Name, queued_at timestamp)
    queued_jobs: DashMap<String, (String, u64)>,
    //// `SessionBuilder` for constructing `SessionContext` from stored `BallistaConfig`
    session_builder: SessionBuilder,
}

impl<S: KeyValueStore, T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    KeyValueState<S, T, U>
{
    pub fn new(
        scheduler: impl Into<String>,
        store: S,
        codec: BallistaCodec<T, U>,
        session_builder: SessionBuilder,
    ) -> Self {
        Self {
            store,
            executors: Arc::new(DashMap::new()),
            executor_heartbeats: Arc::new(DashMap::new()),
            scheduler: scheduler.into(),
            codec,
            queued_jobs: DashMap::new(),
            session_builder,
        }
    }

    /// Initialize the set of active executor heartbeats from storage
    async fn init_active_executor_heartbeats(&self) -> Result<()> {
        let heartbeats = self.store.scan(Keyspace::Heartbeats, None).await?;

        for (_, value) in heartbeats {
            let data: ExecutorHeartbeat = decode_protobuf(&value)?;
            if let Some(protobuf::ExecutorStatus {
                status: Some(protobuf::executor_status::Status::Active(_)),
            }) = &data.status
            {
                self.executor_heartbeats
                    .insert(data.executor_id.clone(), data);
            }
        }

        Ok(())
    }

    /// Return the stream of executor heartbeats observed by all schedulers in the cluster.
    /// This can be aggregated to provide an eventually consistent view of all executors within the cluster
    async fn executor_heartbeat_stream(&self) -> Result<ExecutorHeartbeatStream> {
        let events = self
            .store
            .watch(Keyspace::Heartbeats, String::default())
            .await?;

        Ok(events
            .filter_map(|event| {
                futures::future::ready(match event {
                    WatchEvent::Put(_, value) => {
                        if let Ok(heartbeat) =
                            decode_protobuf::<ExecutorHeartbeat>(&value)
                        {
                            Some(heartbeat)
                        } else {
                            None
                        }
                    }
                    WatchEvent::Delete(_) => None,
                })
            })
            .boxed())
    }

    /// Get the topology nodes of the cluster for consistent hashing
    fn get_topology_nodes(
        &self,
        available_slots: &[AvailableTaskSlots],
        executors: Option<HashSet<String>>,
    ) -> HashMap<String, TopologyNode> {
        let mut nodes: HashMap<String, TopologyNode> = HashMap::new();
        for slots in available_slots {
            if let Some(executors) = executors.as_ref() {
                if !executors.contains(&slots.executor_id) {
                    continue;
                }
            }
            if let Some(executor) = self.executors.get(&slots.executor_id) {
                let node = TopologyNode::new(
                    &executor.host,
                    executor.port,
                    &slots.executor_id,
                    self.executor_heartbeats
                        .get(&executor.id)
                        .map(|heartbeat| heartbeat.timestamp)
                        .unwrap_or(0),
                    slots.slots,
                );
                if let Some(existing_node) = nodes.get(node.name()) {
                    if existing_node.last_seen_ts < node.last_seen_ts {
                        nodes.insert(node.name().to_string(), node);
                    }
                } else {
                    nodes.insert(node.name().to_string(), node);
                }
            }
        }
        nodes
    }
}

#[async_trait]
impl<S: KeyValueStore, T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    ClusterState for KeyValueState<S, T, U>
{
    /// Initialize a background process that will listen for executor heartbeats and update the in-memory cache
    /// of executor heartbeats
    async fn init(&self) -> Result<()> {
        self.init_active_executor_heartbeats().await?;

        let mut heartbeat_stream = self.executor_heartbeat_stream().await?;

        info!("Initializing heartbeat listener");

        let heartbeats = self.executor_heartbeats.clone();
        let executors = self.executors.clone();
        tokio::task::spawn(async move {
            while let Some(heartbeat) = heartbeat_stream.next().await {
                let executor_id = heartbeat.executor_id.clone();

                match heartbeat
                    .status
                    .as_ref()
                    .and_then(|status| status.status.as_ref())
                {
                    Some(protobuf::executor_status::Status::Dead(_)) => {
                        heartbeats.remove(&executor_id);
                        executors.remove(&executor_id);
                    }
                    _ => {
                        heartbeats.insert(executor_id, heartbeat);
                    }
                }
            }
        });

        Ok(())
    }

    async fn bind_schedulable_tasks(
        &self,
        distribution: TaskDistributionPolicy,
        active_jobs: Arc<HashMap<String, JobInfoCache>>,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<BoundTask>> {
        let lock = self.store.lock(Keyspace::Slots, "all").await?;

        with_lock(lock, async {
            let resources = self.store.get(Keyspace::Slots, "all").await?;

            let mut slots =
                ExecutorTaskSlots::decode(resources.as_slice()).map_err(|err| {
                    BallistaError::Internal(format!(
                        "Unexpected value in executor slots state: {err:?}"
                    ))
                })?;

            let available_slots: Vec<&mut AvailableTaskSlots> = slots
                .task_slots
                .iter_mut()
                .filter_map(|data| {
                    (data.slots > 0
                        && executors
                            .as_ref()
                            .map(|executors| executors.contains(&data.executor_id))
                            .unwrap_or(true))
                    .then_some(data)
                })
                .collect();

            let bound_tasks = match distribution {
                TaskDistributionPolicy::Bias => {
                    bind_task_bias(available_slots, active_jobs, |_| false).await
                }
                TaskDistributionPolicy::RoundRobin => {
                    bind_task_round_robin(available_slots, active_jobs, |_| false).await
                }
                TaskDistributionPolicy::ConsistentHash {
                    num_replicas,
                    tolerance,
                } => {
                    let mut bound_tasks = bind_task_round_robin(
                        available_slots,
                        active_jobs.clone(),
                        |stage_plan: Arc<dyn ExecutionPlan>| {
                            if let Ok(scan_files) = get_scan_files(stage_plan) {
                                // Should be opposite to consistent hash ones.
                                !is_skip_consistent_hash(&scan_files)
                            } else {
                                false
                            }
                        },
                    )
                    .await;
                    info!("{} tasks bound by round robin policy", bound_tasks.len());
                    let (bound_tasks_consistent_hash, ch_topology) =
                        bind_task_consistent_hash(
                            self.get_topology_nodes(&slots.task_slots, executors),
                            num_replicas,
                            tolerance,
                            active_jobs,
                            |_, plan| get_scan_files(plan),
                        )
                        .await?;
                    info!(
                        "{} tasks bound by consistent hashing policy",
                        bound_tasks_consistent_hash.len()
                    );
                    if !bound_tasks_consistent_hash.is_empty() {
                        bound_tasks.extend(bound_tasks_consistent_hash);
                        // Update the available slots
                        let mut executor_data: HashMap<String, AvailableTaskSlots> =
                            slots
                                .task_slots
                                .into_iter()
                                .map(|slots| (slots.executor_id.clone(), slots))
                                .collect();
                        let ch_topology = ch_topology.unwrap();
                        for node in ch_topology.nodes() {
                            if let Some(data) = executor_data.get_mut(&node.id) {
                                data.slots = node.available_slots;
                            } else {
                                error!("Fail to find executor data for {}", &node.id);
                            }
                        }
                        slots.task_slots = executor_data.into_values().collect();
                    }
                    bound_tasks
                }
            };

            if !bound_tasks.is_empty() {
                self.store
                    .put(Keyspace::Slots, "all".to_owned(), slots.encode_to_vec())
                    .await?
            }

            Ok(bound_tasks)
        })
        .await
    }

    async fn unbind_tasks(&self, executor_slots: Vec<ExecutorSlot>) -> Result<()> {
        let mut increments = HashMap::new();
        for (executor_id, num_slots) in executor_slots {
            let v = increments.entry(executor_id).or_insert_with(|| 0);
            *v += num_slots;
        }

        let lock = self.store.lock(Keyspace::Slots, "all").await?;

        with_lock(lock, async {
            let resources = self.store.get(Keyspace::Slots, "all").await?;

            let mut slots =
                ExecutorTaskSlots::decode(resources.as_slice()).map_err(|err| {
                    BallistaError::Internal(format!(
                        "Unexpected value in executor slots state: {err:?}"
                    ))
                })?;

            for executor_slots in slots.task_slots.iter_mut() {
                if let Some(slots) = increments.get(&executor_slots.executor_id) {
                    executor_slots.slots += *slots;
                }
            }

            self.store
                .put(Keyspace::Slots, "all".to_string(), slots.encode_to_vec())
                .await
        })
        .await
    }

    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        spec: ExecutorData,
    ) -> Result<()> {
        let executor_id = metadata.id.clone();

        //TODO this should be in a transaction
        // Now that we know we can connect, save the metadata and slots
        self.save_executor_metadata(metadata).await?;
        self.save_executor_heartbeat(ExecutorHeartbeat {
            executor_id: executor_id.clone(),
            timestamp: timestamp_secs(),
            metrics: vec![],
            status: Some(protobuf::ExecutorStatus {
                status: Some(
                    protobuf::executor_status::Status::Active(String::default()),
                ),
            }),
        })
        .await?;

        let available_slots = AvailableTaskSlots {
            executor_id,
            slots: spec.available_task_slots,
        };

        let lock = self.store.lock(Keyspace::Slots, "all").await?;

        with_lock(lock, async {
            let current_slots = self.store.get(Keyspace::Slots, "all").await?;

            let mut current_slots: ExecutorTaskSlots =
                decode_protobuf(current_slots.as_slice())?;

            if let Some((idx, _)) = current_slots
                .task_slots
                .iter()
                .find_position(|slots| slots.executor_id == available_slots.executor_id)
            {
                current_slots.task_slots[idx] = available_slots;
            } else {
                current_slots.task_slots.push(available_slots);
            }

            self.store
                .put(
                    Keyspace::Slots,
                    "all".to_string(),
                    current_slots.encode_to_vec(),
                )
                .await
        })
        .await?;

        Ok(())
    }

    async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        let executor_id = metadata.id.clone();

        let proto: protobuf::ExecutorMetadata = metadata.clone().into();
        self.store
            .put(
                Keyspace::Executors,
                executor_id.clone(),
                proto.encode_to_vec(),
            )
            .await?;

        self.executors.insert(executor_id, metadata);

        Ok(())
    }

    async fn get_executor_metadata(&self, executor_id: &str) -> Result<ExecutorMetadata> {
        let metadata = if let Some(metadata) = self.executors.get(executor_id) {
            metadata.value().clone()
        } else {
            let value = self.store.get(Keyspace::Executors, executor_id).await?;
            if value.is_empty() {
                return Err(BallistaError::Internal(format!(
                    "Executor {} not registered",
                    executor_id
                )));
            }
            let decoded =
                decode_into::<protobuf::ExecutorMetadata, ExecutorMetadata>(&value)?;
            self.executors
                .insert(executor_id.to_string(), decoded.clone());

            decoded
        };

        Ok(metadata)
    }

    async fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) -> Result<()> {
        let executor_id = heartbeat.executor_id.clone();
        self.store
            .put(
                Keyspace::Heartbeats,
                executor_id.clone(),
                heartbeat.clone().encode_to_vec(),
            )
            .await?;
        self.executor_heartbeats.insert(executor_id, heartbeat);
        Ok(())
    }

    async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        let value = ExecutorHeartbeat {
            executor_id: executor_id.to_owned(),
            timestamp: timestamp_secs(),
            metrics: vec![],
            status: Some(protobuf::ExecutorStatus {
                status: Some(protobuf::executor_status::Status::Dead("".to_string())),
            }),
        }
        .encode_to_vec();

        self.store
            .put(Keyspace::Heartbeats, executor_id.to_owned(), value)
            .await?;
        self.executor_heartbeats.remove(executor_id);

        // TODO Check the Executor reservation logic for push-based scheduling

        Ok(())
    }

    fn executor_heartbeats(&self) -> HashMap<String, ExecutorHeartbeat> {
        self.executor_heartbeats
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    fn get_executor_heartbeat(&self, executor_id: &str) -> Option<ExecutorHeartbeat> {
        self.executor_heartbeats
            .get(executor_id)
            .map(|r| r.value().clone())
    }
}

#[async_trait]
impl<S: KeyValueStore, T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> JobState
    for KeyValueState<S, T, U>
{
    fn accept_job(&self, job_id: &str, job_name: &str, queued_at: u64) -> Result<()> {
        self.queued_jobs
            .insert(job_id.to_string(), (job_name.to_string(), queued_at));

        Ok(())
    }

    fn pending_job_number(&self) -> usize {
        self.queued_jobs.len()
    }

    async fn submit_job(&self, job_id: String, graph: &ExecutionGraph) -> Result<()> {
        if self.queued_jobs.get(&job_id).is_some() {
            let status = graph.status();
            let encoded_graph =
                ExecutionGraph::encode_execution_graph(graph.clone(), &self.codec)?;

            self.store
                .apply_txn(vec![
                    (
                        Operation::Put(status.encode_to_vec()),
                        Keyspace::JobStatus,
                        job_id.clone(),
                    ),
                    (
                        Operation::Put(encoded_graph.encode_to_vec()),
                        Keyspace::ExecutionGraph,
                        job_id.clone(),
                    ),
                ])
                .await?;

            self.queued_jobs.remove(&job_id);

            Ok(())
        } else {
            Err(BallistaError::Internal(format!(
                "Failed to submit job {job_id}, job was not in queueud jobs"
            )))
        }
    }

    async fn get_jobs(&self) -> Result<HashSet<String>> {
        self.store.scan_keys(Keyspace::JobStatus).await
    }

    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if let Some((job_name, queued_at)) = self.queued_jobs.get(job_id).as_deref() {
            Ok(Some(JobStatus {
                job_id: job_id.to_string(),
                job_name: job_name.clone(),
                status: Some(Status::Queued(QueuedJob {
                    queued_at: *queued_at,
                })),
            }))
        } else {
            let value = self.store.get(Keyspace::JobStatus, job_id).await?;

            (!value.is_empty())
                .then(|| decode_protobuf(value.as_slice()))
                .transpose()
        }
    }

    async fn get_execution_graph(&self, job_id: &str) -> Result<Option<ExecutionGraph>> {
        let value = self.store.get(Keyspace::ExecutionGraph, job_id).await?;

        if value.is_empty() {
            return Ok(None);
        }

        let proto: protobuf::ExecutionGraph = decode_protobuf(value.as_slice())?;

        let session = self.get_session(&proto.session_id).await?;

        Ok(Some(
            ExecutionGraph::decode_execution_graph(proto, &self.codec, session.as_ref())
                .await?,
        ))
    }

    async fn save_job(&self, job_id: &str, graph: &ExecutionGraph) -> Result<()> {
        let status = graph.status();
        let encoded_graph =
            ExecutionGraph::encode_execution_graph(graph.clone(), &self.codec)?;

        self.store
            .apply_txn(vec![
                (
                    Operation::Put(status.encode_to_vec()),
                    Keyspace::JobStatus,
                    job_id.to_string(),
                ),
                (
                    Operation::Put(encoded_graph.encode_to_vec()),
                    Keyspace::ExecutionGraph,
                    job_id.to_string(),
                ),
            ])
            .await
    }

    async fn fail_unscheduled_job(&self, job_id: &str, reason: String) -> Result<()> {
        if let Some((job_id, (job_name, queued_at))) = self.queued_jobs.remove(job_id) {
            let status = JobStatus {
                job_id: job_id.clone(),
                job_name,
                status: Some(Status::Failed(FailedJob {
                    error: reason,
                    queued_at,
                    started_at: 0,
                    ended_at: 0,
                })),
            };

            self.store
                .put(Keyspace::JobStatus, job_id, status.encode_to_vec())
                .await
        } else {
            Err(BallistaError::Internal(format!(
                "Could not fail unscheduled job {job_id}, not found in queued jobs"
            )))
        }
    }

    async fn remove_job(&self, job_id: &str) -> Result<()> {
        if self.queued_jobs.remove(job_id).is_none() {
            self.store
                .apply_txn(vec![
                    (Operation::Delete, Keyspace::JobStatus, job_id.to_string()),
                    (
                        Operation::Delete,
                        Keyspace::ExecutionGraph,
                        job_id.to_string(),
                    ),
                ])
                .await
        } else {
            Ok(())
        }
    }

    async fn try_acquire_job(&self, _job_id: &str) -> Result<Option<ExecutionGraph>> {
        Err(BallistaError::NotImplemented(
            "Work stealing is not currently implemented".to_string(),
        ))
    }

    async fn job_state_events(&self) -> Result<JobStateEventStream> {
        let watch = self
            .store
            .watch(Keyspace::JobStatus, String::default())
            .await?;

        let stream = watch
            .filter_map(|event| {
                futures::future::ready(match event {
                    WatchEvent::Put(key, value) => {
                        if let Some(job_id) = Keyspace::JobStatus.strip_prefix(&key) {
                            match JobStatus::decode(value.as_slice()) {
                                Ok(status) => Some(JobStateEvent::JobUpdated {
                                    job_id: job_id.to_string(),
                                    status,
                                }),
                                Err(err) => {
                                    warn!(
                                    "Error decoding job status from watch event: {err:?}"
                                );
                                    None
                                }
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
            })
            .boxed();

        Ok(stream)
    }

    async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        let value = self.store.get(Keyspace::Sessions, session_id).await?;

        let settings: protobuf::SessionSettings = decode_protobuf(&value)?;

        let mut config_builder = BallistaConfig::builder();
        for kv_pair in &settings.configs {
            config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
        }
        let config = config_builder.build()?;

        Ok(create_datafusion_context(&config, self.session_builder))
    }

    async fn create_session(
        &self,
        config: &BallistaConfig,
    ) -> Result<Arc<SessionContext>> {
        let mut settings: Vec<KeyValuePair> = vec![];

        for (key, value) in config.settings() {
            settings.push(KeyValuePair {
                key: key.clone(),
                value: value.clone(),
            })
        }

        let value = protobuf::SessionSettings { configs: settings };

        let session = create_datafusion_context(config, self.session_builder);

        self.store
            .put(
                Keyspace::Sessions,
                session.session_id(),
                value.encode_to_vec(),
            )
            .await?;

        Ok(session)
    }

    async fn update_session(
        &self,
        session_id: &str,
        config: &BallistaConfig,
    ) -> Result<Arc<SessionContext>> {
        let mut settings: Vec<KeyValuePair> = vec![];

        for (key, value) in config.settings() {
            settings.push(KeyValuePair {
                key: key.clone(),
                value: value.clone(),
            })
        }

        let value = protobuf::SessionSettings { configs: settings };
        self.store
            .put(
                Keyspace::Sessions,
                session_id.to_owned(),
                value.encode_to_vec(),
            )
            .await?;

        Ok(create_datafusion_context(config, self.session_builder))
    }

    async fn remove_session(
        &self,
        session_id: &str,
    ) -> Result<Option<Arc<SessionContext>>> {
        let session_ctx = self.get_session(session_id).await.ok();

        self.store.delete(Keyspace::Sessions, session_id).await?;

        Ok(session_ctx)
    }
}

async fn with_lock<Out, F: Future<Output = Out>>(mut lock: Box<dyn Lock>, op: F) -> Out {
    let result = op.await;
    lock.unlock().await;
    result
}

#[cfg(test)]
mod test {

    use crate::cluster::kv::KeyValueState;
    use crate::cluster::storage::sled::SledClient;
    use crate::cluster::test_util::{test_job_lifecycle, test_job_planning_failure};
    use crate::test_utils::{
        test_aggregation_plan, test_join_plan, test_two_aggregations_plan,
    };
    use ballista_core::error::Result;
    use ballista_core::serde::BallistaCodec;
    use ballista_core::utils::default_session_builder;

    #[cfg(feature = "sled")]
    #[tokio::test]
    async fn test_sled_job_lifecycle() -> Result<()> {
        test_job_lifecycle(make_sled_state()?, test_aggregation_plan(4).await).await?;
        test_job_lifecycle(make_sled_state()?, test_two_aggregations_plan(4).await)
            .await?;
        test_job_lifecycle(make_sled_state()?, test_join_plan(4).await).await?;
        Ok(())
    }

    #[cfg(feature = "sled")]
    #[tokio::test]
    async fn test_in_memory_job_planning_failure() -> Result<()> {
        test_job_planning_failure(make_sled_state()?, test_aggregation_plan(4).await)
            .await?;
        test_job_planning_failure(
            make_sled_state()?,
            test_two_aggregations_plan(4).await,
        )
        .await?;
        test_job_planning_failure(make_sled_state()?, test_join_plan(4).await).await?;

        Ok(())
    }

    #[cfg(feature = "sled")]
    fn make_sled_state() -> Result<KeyValueState<SledClient>> {
        Ok(KeyValueState::new(
            "",
            SledClient::try_new_temporary()?,
            BallistaCodec::default(),
            default_session_builder,
        ))
    }
}
