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

use crate::auth::default_handshake_authorizer;
use crate::cluster::BallistaCluster;
use crate::config::SchedulerConfig;
use crate::metrics::default_metrics_collector;
use crate::{cluster::storage::sled::SledClient, scheduler_server::SchedulerServer};
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::{create_grpc_server, default_session_builder};
use ballista_core::{
    error::Result, serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer,
    BALLISTA_VERSION,
};
use datafusion_proto::protobuf::LogicalPlanNode;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

pub async fn new_standalone_scheduler() -> Result<SocketAddr> {
    let metrics_collector = default_metrics_collector()?;
    let handshake_authorizer = default_handshake_authorizer();

    let cluster = BallistaCluster::new_kv(
        SledClient::try_new_temporary()?,
        "localhost:50050",
        default_session_builder,
        BallistaCodec::default(),
    );

    let mut scheduler_server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        SchedulerServer::new(
            "localhost:50050".to_owned(),
            cluster,
            BallistaCodec::default(),
            Arc::new(SchedulerConfig::default()),
            metrics_collector,
            handshake_authorizer,
        );

    scheduler_server.init().await?;
    let server = SchedulerGrpcServer::new(scheduler_server.clone());
    // Let the OS assign a random, free port
    let listener = TcpListener::bind("localhost:0").await?;
    let addr = listener.local_addr()?;
    info!(
        "Ballista v{} Rust Scheduler listening on {:?}",
        BALLISTA_VERSION, addr
    );
    tokio::spawn(
        create_grpc_server()
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                listener,
            )),
    );

    Ok(addr)
}
