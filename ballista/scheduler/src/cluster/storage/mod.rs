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

#[cfg(feature = "etcd")]
pub mod etcd;
#[cfg(feature = "sled")]
pub mod sled;

use async_trait::async_trait;
use ballista_core::error::Result;
use futures::{future, Stream};
use std::collections::HashSet;
use tokio::sync::OwnedMutexGuard;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Keyspace {
    Executors,
    JobStatus,
    ExecutionGraph,
    ActiveJobs,
    CompletedJobs,
    FailedJobs,
    Slots,
    Sessions,
    Heartbeats,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum Operation {
    Put(Vec<u8>),
    Delete,
}

/// A trait that defines a KeyValue interface with basic locking primitives for persisting Ballista cluster state
#[async_trait]
pub trait KeyValueStore: Send + Sync + Clone {
    /// Retrieve the data associated with a specific key in a given keyspace.
    ///
    /// An empty vec is returned if the key does not exist.
    async fn get(&self, keyspace: Keyspace, key: &str) -> Result<Vec<u8>>;

    /// Retrieve all key/value pairs in given keyspace matching a given key prefix.
    async fn get_from_prefix(
        &self,
        keyspace: Keyspace,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>>;

    /// Retrieve all key/value pairs in a given keyspace. If a limit is specified, will return at
    /// most `limit` key-value pairs.
    async fn scan(
        &self,
        keyspace: Keyspace,
        limit: Option<usize>,
    ) -> Result<Vec<(String, Vec<u8>)>>;

    /// Retrieve all keys from a given keyspace (without their values). The implementations
    /// should handle stripping any prefixes it may add.
    async fn scan_keys(&self, keyspace: Keyspace) -> Result<HashSet<String>>;

    /// Saves the value into the provided key, overriding any previous data that might have been associated to that key.
    async fn put(&self, keyspace: Keyspace, key: String, value: Vec<u8>) -> Result<()>;

    /// Bundle multiple operation in a single transaction. Either all values should be saved, or all should fail.
    /// It can support multiple types of operations and keyspaces. If the count of the unique keyspace is more than one,
    /// more than one locks has to be acquired.
    async fn apply_txn(&self, ops: Vec<(Operation, Keyspace, String)>) -> Result<()>;
    /// Acquire mutex with specified IDs.
    async fn acquire_locks(
        &self,
        mut ids: Vec<(Keyspace, &str)>,
    ) -> Result<Vec<Box<dyn Lock>>> {
        // We always acquire locks in a specific order to avoid deadlocks.
        ids.sort_by_key(|n| format!("/{:?}/{}", n.0, n.1));
        future::try_join_all(ids.into_iter().map(|(ks, key)| self.lock(ks, key))).await
    }

    /// Atomically move the given key from one keyspace to another
    async fn mv(
        &self,
        from_keyspace: Keyspace,
        to_keyspace: Keyspace,
        key: &str,
    ) -> Result<()>;

    /// Acquire mutex with specified ID.
    async fn lock(&self, keyspace: Keyspace, key: &str) -> Result<Box<dyn Lock>>;

    /// Watch all events that happen on a specific prefix.
    async fn watch(
        &self,
        keyspace: Keyspace,
        prefix: String,
    ) -> Result<Box<dyn Watch<Item = WatchEvent>>>;

    /// Permanently delete a key from state
    async fn delete(&self, keyspace: Keyspace, key: &str) -> Result<()>;
}

/// A Watch is a cancelable stream of put or delete events in the [StateBackendClient]
#[async_trait]
pub trait Watch: Stream<Item = WatchEvent> + Send + Unpin {
    async fn cancel(&mut self) -> Result<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchEvent {
    /// Contains the inserted or updated key and the new value
    Put(String, Vec<u8>),

    /// Contains the deleted key
    Delete(String),
}

#[async_trait]
pub trait Lock: Send + Sync {
    async fn unlock(&mut self);
}

#[async_trait]
impl<T: Send + Sync> Lock for OwnedMutexGuard<T> {
    async fn unlock(&mut self) {}
}
