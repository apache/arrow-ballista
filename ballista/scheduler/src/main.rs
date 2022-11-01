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

//! Ballista Rust scheduler binary.

use anyhow::{Context, Result};
#[cfg(feature = "flight-sql")]
use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_scheduler::scheduler_server::externalscaler::external_scaler_server::ExternalScalerServer;
use futures::future::{self, Either, TryFutureExt};
use hyper::{server::conn::AddrStream, service::make_service_fn, Server};
use std::convert::Infallible;
use std::{env, io, net::SocketAddr, sync::Arc};
use tonic::transport::server::Connected;
use tower::Service;

use ballista_core::BALLISTA_VERSION;
use ballista_core::{
    print_version,
    serde::protobuf::{scheduler_grpc_server::SchedulerGrpcServer, PhysicalPlanNode},
};
use ballista_scheduler::api::{get_routes, EitherBody, Error};
#[cfg(feature = "etcd")]
use ballista_scheduler::state::backend::etcd::EtcdClient;
#[cfg(feature = "sled")]
use ballista_scheduler::state::backend::standalone::StandaloneClient;
use datafusion_proto::protobuf::LogicalPlanNode;

use ballista_scheduler::scheduler_server::SchedulerServer;
use ballista_scheduler::state::backend::{StateBackend, StateBackendClient};

use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::serde::BallistaCodec;
use log::info;

#[macro_use]
extern crate configure_me;

#[allow(clippy::all, warnings)]
mod config {
    // Ideally we would use the include_config macro from configure_me, but then we cannot use
    // #[allow(clippy::all)] to silence clippy warnings from the generated code
    include!(concat!(
        env!("OUT_DIR"),
        "/scheduler_configure_me_config.rs"
    ));
}

use ballista_core::utils::{create_grpc_server, TableProviderSessionBuilder};
use ballista_scheduler::config::SlotsPolicy;
#[cfg(feature = "flight-sql")]
use ballista_scheduler::flight_sql::FlightSqlServiceImpl;
use config::prelude::*;
use datafusion::datasource::datasource::TableProviderFactory;
#[cfg(feature = "delta")]
use deltalake::delta_datafusion::{
    DeltaLogicalCodec, DeltaPhysicalCodec, DeltaTableFactory,
};
use tracing_subscriber::EnvFilter;

async fn start_server(
    scheduler_name: String,
    config_backend: Arc<dyn StateBackendClient>,
    addr: SocketAddr,
    scheduling_policy: TaskSchedulingPolicy,
    slots_policy: SlotsPolicy,
    event_loop_buffer_size: usize,
    advertise_endpoint: Option<String>,
) -> Result<()> {
    info!(
        "Ballista v{} Scheduler listening on {:?}",
        BALLISTA_VERSION, addr
    );
    // Should only call SchedulerServer::new() once in the process
    info!(
        "Starting Scheduler grpc server with task scheduling policy of {:?}",
        scheduling_policy
    );

    #[cfg(feature = "delta")]
    let session_builder = {
        let factory: Arc<(dyn TableProviderFactory + 'static)> =
            Arc::new(DeltaTableFactory {});
        let factories =
            std::collections::HashMap::from([("deltatable".to_string(), factory)]);
        TableProviderSessionBuilder::new(factories)
    };
    #[cfg(not(feature = "delta"))]
    let session_builder = DefaultSessionBuilder {};

    #[cfg(feature = "delta")]
    let codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> = BallistaCodec::new(
        Arc::new(DeltaLogicalCodec {}),
        Arc::new(DeltaPhysicalCodec {}),
    );
    #[cfg(not(feature = "delta"))]
    let codec = BallistaCodec::default();

    let mut scheduler_server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        SchedulerServer::new_with_policy(
            scheduler_name,
            config_backend.clone(),
            scheduling_policy,
            slots_policy,
            codec,
            Arc::new(session_builder),
            event_loop_buffer_size,
            advertise_endpoint,
        );

    scheduler_server.init().await?;

    Server::bind(&addr)
        .serve(make_service_fn(move |request: &AddrStream| {
            let scheduler_grpc_server =
                SchedulerGrpcServer::new(scheduler_server.clone());

            let keda_scaler = ExternalScalerServer::new(scheduler_server.clone());

            let tonic_builder = create_grpc_server()
                .add_service(scheduler_grpc_server)
                .add_service(keda_scaler);

            #[cfg(feature = "flight-sql")]
            let tonic_builder = tonic_builder.add_service(FlightServiceServer::new(
                FlightSqlServiceImpl::new(scheduler_server.clone()),
            ));

            let mut tonic = tonic_builder.into_service();

            let mut warp = warp::service(get_routes(scheduler_server.clone()));

            let connect_info = request.connect_info();
            future::ok::<_, Infallible>(tower::service_fn(
                move |req: hyper::Request<hyper::Body>| {
                    // Set the connect info from hyper to tonic
                    let (mut parts, body) = req.into_parts();
                    parts.extensions.insert(connect_info.clone());
                    let req = http::Request::from_parts(parts, body);

                    let header = req.headers().get(hyper::header::ACCEPT);
                    if header.is_some() && header.unwrap().eq("application/json") {
                        return Either::Left(
                            warp.call(req)
                                .map_ok(|res| res.map(EitherBody::Left))
                                .map_err(Error::from),
                        );
                    }
                    Either::Right(
                        tonic
                            .call(req)
                            .map_ok(|res| res.map(EitherBody::Right))
                            .map_err(Error::from),
                    )
                },
            ))
        }))
        .await
        .context("Could not start grpc server")
}

#[tokio::main]
async fn main() -> Result<()> {
    // parse options
    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/scheduler.toml"])
            .unwrap_or_exit();

    if opt.version {
        print_version();
        std::process::exit(0);
    }

    let special_mod_log_level = opt.log_level_setting;
    let namespace = opt.namespace;
    let external_host = opt.external_host;
    let bind_host = opt.bind_host;
    let port = opt.bind_port;
    let log_dir = opt.log_dir;
    let print_thread_info = opt.print_thread_info;
    let log_file_name_prefix =
        format!("scheduler_{}_{}_{}", namespace, external_host, port);
    let scheduler_name = format!("{}:{}", external_host, port);

    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or(special_mod_log_level));
    // File layer
    if let Some(log_dir) = log_dir {
        let log_file = tracing_appender::rolling::daily(log_dir, &log_file_name_prefix);
        tracing_subscriber::fmt()
            .with_ansi(true)
            .with_thread_names(print_thread_info)
            .with_thread_ids(print_thread_info)
            .with_writer(log_file)
            .with_env_filter(log_filter)
            .init();
    } else {
        // Console layer
        tracing_subscriber::fmt()
            .with_ansi(true)
            .with_thread_names(print_thread_info)
            .with_thread_ids(print_thread_info)
            .with_writer(io::stdout)
            .with_env_filter(log_filter)
            .init();
    }

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr.parse()?;

    let client: Arc<dyn StateBackendClient> = match opt.config_backend {
        #[cfg(not(any(feature = "sled", feature = "etcd")))]
        _ => std::compile_error!(
            "To build the scheduler enable at least one config backend feature (`etcd` or `sled`)"
        ),
        #[cfg(feature = "etcd")]
        StateBackend::Etcd => {
            let etcd = etcd_client::Client::connect(&[opt.etcd_urls], None)
                .await
                .context("Could not connect to etcd")?;
            Arc::new(EtcdClient::new(namespace.clone(), etcd))
        }
        #[cfg(not(feature = "etcd"))]
        StateBackend::Etcd => {
            unimplemented!(
                "build the scheduler with the `etcd` feature to use the etcd config backend"
            )
        }
        #[cfg(feature = "sled")]
        StateBackend::Standalone => {
            if opt.sled_dir.is_empty() {
                Arc::new(
                    StandaloneClient::try_new_temporary()
                        .context("Could not create standalone config backend")?,
                )
            } else {
                println!("{}", opt.sled_dir);
                Arc::new(
                    StandaloneClient::try_new(opt.sled_dir)
                        .context("Could not create standalone config backend")?,
                )
            }
        }
        #[cfg(not(feature = "sled"))]
        StateBackend::Standalone => {
            unimplemented!(
                "build the scheduler with the `sled` feature to use the standalone config backend"
            )
        }
    };

    let scheduling_policy: TaskSchedulingPolicy = opt.scheduler_policy;
    let slots_policy: SlotsPolicy = opt.executor_slots_policy;
    let event_loop_buffer_size = opt.event_loop_buffer_size as usize;
    start_server(
        scheduler_name,
        client,
        addr,
        scheduling_policy,
        slots_policy,
        event_loop_buffer_size,
        opt.advertise_endpoint,
    )
    .await?;
    Ok(())
}
