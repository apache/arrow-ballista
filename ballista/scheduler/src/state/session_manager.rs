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

use crate::state::backend::{Keyspace, StateBackendClient};
use crate::state::{decode_protobuf, encode_protobuf};
use ballista_core::config::BallistaConfig;
use ballista_core::error::Result;
use ballista_core::serde::protobuf::{self, KeyValuePair};
use datafusion::prelude::{SessionConfig, SessionContext};

use ballista_core::utils::SessionBuilder;
use datafusion::common::ScalarValue;
use log::warn;
use std::sync::Arc;

#[derive(Clone)]
pub struct SessionManager {
    state: Arc<dyn StateBackendClient>,
    session_builder: Arc<dyn SessionBuilder>,
}

impl SessionManager {
    pub fn new(
        state: Arc<dyn StateBackendClient>,
        session_builder: Arc<dyn SessionBuilder>,
    ) -> Self {
        let session_builder = session_builder;
        Self {
            state,
            session_builder,
        }
    }

    pub async fn update_session(
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

        let value = encode_protobuf(&protobuf::SessionSettings { configs: settings })?;
        self.state
            .put(Keyspace::Sessions, session_id.to_owned(), value)
            .await?;

        create_datafusion_context(config, self.session_builder.as_ref())
    }

    pub async fn create_session(
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

        let mut config_builder = BallistaConfig::builder();
        for kv_pair in &settings {
            config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
        }
        let config = config_builder.build()?;

        let ctx = create_datafusion_context(&config, self.session_builder.as_ref())?;

        let value = encode_protobuf(&protobuf::SessionSettings { configs: settings })?;
        self.state
            .put(Keyspace::Sessions, ctx.session_id(), value)
            .await?;

        Ok(ctx)
    }

    pub async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        let value = self.state.get(Keyspace::Sessions, session_id).await?;

        let settings: protobuf::SessionSettings = decode_protobuf(&value)?;

        let mut config_builder = BallistaConfig::builder();
        for kv_pair in &settings.configs {
            config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
        }
        let config = config_builder.build()?;

        create_datafusion_context(&config, self.session_builder.as_ref())
    }
}

/// Create a DataFusion session context that is compatible with Ballista Configuration
pub fn create_datafusion_context(
    ballista_config: &BallistaConfig,
    session_builder: &dyn SessionBuilder,
) -> Result<Arc<SessionContext>> {
    let config = SessionConfig::new()
        .with_target_partitions(ballista_config.default_shuffle_partitions())
        .with_batch_size(ballista_config.default_batch_size())
        .with_repartition_joins(ballista_config.repartition_joins())
        .with_repartition_aggregations(ballista_config.repartition_aggregations())
        .with_repartition_windows(ballista_config.repartition_windows())
        .with_parquet_pruning(ballista_config.parquet_pruning());
    let config = propagate_ballista_configs(config, ballista_config);

    let session_state = session_builder.build(config)?;
    Ok(Arc::new(SessionContext::with_state(session_state)))
}

/// Update the existing DataFusion session context with Ballista Configuration
pub fn update_datafusion_context(
    session_ctx: Arc<SessionContext>,
    ballista_config: &BallistaConfig,
) -> Arc<SessionContext> {
    {
        let mut mut_state = session_ctx.state.write();
        // TODO Currently we have to start from default session config due to the interface not support update
        let config = SessionConfig::default()
            .with_target_partitions(ballista_config.default_shuffle_partitions())
            .with_batch_size(ballista_config.default_batch_size())
            .with_repartition_joins(ballista_config.repartition_joins())
            .with_repartition_aggregations(ballista_config.repartition_aggregations())
            .with_repartition_windows(ballista_config.repartition_windows())
            .with_parquet_pruning(ballista_config.parquet_pruning());
        let config = propagate_ballista_configs(config, ballista_config);
        mut_state.config = config;
    }
    session_ctx
}

fn propagate_ballista_configs(
    config: SessionConfig,
    ballista_config: &BallistaConfig,
) -> SessionConfig {
    let mut config = config;
    // TODO we cannot just pass string values along to DataFusion configs
    // and we will need to improve that in the next release of DataFusion
    // see https://github.com/apache/arrow-datafusion/issues/3500
    for (k, v) in ballista_config.settings() {
        // see https://arrow.apache.org/datafusion/user-guide/configs.html for explanation of these configs
        match k.as_str() {
            "datafusion.optimizer.filter_null_join_keys" => {
                config = config.set(
                    k,
                    ScalarValue::Boolean(Some(v.parse::<bool>().unwrap_or(false))),
                )
            }
            "datafusion.execution.coalesce_batches" => {
                config = config.set(
                    k,
                    ScalarValue::Boolean(Some(v.parse::<bool>().unwrap_or(true))),
                )
            }
            "datafusion.execution.coalesce_target_batch_size" => {
                config = config.set(
                    k,
                    ScalarValue::UInt64(Some(v.parse::<u64>().unwrap_or(4096))),
                )
            }
            "datafusion.optimizer.skip_failed_rules" => {
                config = config.set(
                    k,
                    ScalarValue::Boolean(Some(v.parse::<bool>().unwrap_or(true))),
                )
            }
            _ => {
                warn!("Ignoring unknown configuration option {} = {}", k, v);
            }
        }
    }
    config
}
