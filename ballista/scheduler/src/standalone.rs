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

use crate::config::SchedulerConfig;
use crate::metrics::default_metrics_collector;
use crate::{
    scheduler_server::SchedulerServer, state::backend::standalone::StandaloneClient,
};
use ballista_core::serde::protobuf::PhysicalPlanNode;
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::{create_grpc_server, SessionBuilder};
use ballista_core::{
    error::Result, serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer,
    BALLISTA_VERSION,
};
use datafusion_proto::protobuf::LogicalPlanNode;
use log::info;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

pub async fn new_standalone_scheduler(
    session_builder: Arc<dyn SessionBuilder>,
) -> Result<SocketAddr> {
    let client = StandaloneClient::try_new_temporary()?;

    let metrics_collector = default_metrics_collector()?;

    let mut scheduler_server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        SchedulerServer::new(
            "localhost:50050".to_owned(),
            Arc::new(client),
            BallistaCodec::default(),
            session_builder,
            SchedulerConfig::default(),
            metrics_collector,
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
