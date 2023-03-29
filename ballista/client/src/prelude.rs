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

//! Ballista Prelude (common imports)

pub use ballista_core::{
    config::{
        BallistaConfig, BALLISTA_DEFAULT_BATCH_SIZE, BALLISTA_DEFAULT_SHUFFLE_PARTITIONS,
        BALLISTA_GRPC_CONNECTION_TIMEOUT, BALLISTA_JOB_NAME, BALLISTA_PARQUET_PRUNING,
        BALLISTA_PLUGIN_DIR, BALLISTA_REPARTITION_AGGREGATIONS,
        BALLISTA_REPARTITION_JOINS, BALLISTA_REPARTITION_WINDOWS,
        BALLISTA_WITH_INFORMATION_SCHEMA,
    },
    error::{BallistaError, Result},
};

pub use futures::StreamExt;

pub use crate::context::BallistaContext;
