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

use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementUpdate, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, Location, Ticket,
};
use log::{debug, error, warn};
use std::convert::TryFrom;
use std::pin::Pin;
use std::str::FromStr;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status, Streaming};

use crate::scheduler_server::SchedulerServer;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::ProstMessageExt;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::SchemaAsIpc;
use ballista_core::config::BallistaConfig;
use ballista_core::serde::protobuf;
use ballista_core::serde::protobuf::action::ActionType::FetchPartition;
use ballista_core::serde::protobuf::job_status;
use ballista_core::serde::protobuf::JobStatus;
use ballista_core::serde::protobuf::SuccessfulJob;
use ballista_core::utils::create_grpc_client_connection;
use dashmap::DashMap;
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::{IpcDataGenerator, IpcWriteOptions};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::common::batch_byte_size;
use datafusion::prelude::SessionContext;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use prost::Message;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::futures_core::Stream;
use tonic::metadata::MetadataValue;
use uuid::Uuid;

pub struct FlightSqlServiceImpl {
    server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    statements: Arc<DashMap<Uuid, LogicalPlan>>,
    contexts: Arc<DashMap<Uuid, Arc<SessionContext>>>,
}

const TABLE_TYPES: [&str; 2] = ["TABLE", "VIEW"];

impl FlightSqlServiceImpl {
    pub fn new(server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>) -> Self {
        Self {
            server,
            statements: Default::default(),
            contexts: Default::default(),
        }
    }

    #[allow(deprecated)]
    fn tables(&self, ctx: Arc<SessionContext>) -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));
        let tables = ctx.tables()?; // resolved in #501
        let names: Vec<_> = tables.iter().map(|it| Some(it.as_str())).collect();
        let types: Vec<_> = names.iter().map(|_| Some("TABLE")).collect();
        let cats: Vec<_> = names.iter().map(|_| None).collect();
        let schemas: Vec<_> = names.iter().map(|_| None).collect();
        let rb = RecordBatch::try_new(
            schema,
            [cats, schemas, names, types]
                .iter()
                .map(|i| Arc::new(StringArray::from(i.clone())) as ArrayRef)
                .collect::<Vec<_>>(),
        )?;
        Ok(rb)
    }

    fn table_types() -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            [TABLE_TYPES]
                .iter()
                .map(|i| Arc::new(StringArray::from(i.to_vec())) as ArrayRef)
                .collect::<Vec<_>>(),
        )
    }

    async fn create_ctx(&self) -> Result<Uuid, Status> {
        let config_builder = BallistaConfig::builder();
        let config = config_builder
            .build()
            .map_err(|e| Status::internal(format!("Error building config: {e}")))?;
        let ctx = self
            .server
            .state
            .session_manager
            .create_session(&config)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to create SessionContext: {e:?}"))
            })?;
        let handle = Uuid::new_v4();
        self.contexts.insert(handle, ctx);
        Ok(handle)
    }

    fn get_ctx<T>(&self, req: &Request<T>) -> Result<Arc<SessionContext>, Status> {
        let auth = req
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::internal("No authorization header!"))?;
        let str = auth
            .to_str()
            .map_err(|e| Status::internal(format!("Error parsing header: {e}")))?;
        let authorization = str.to_string();
        let bearer = "Bearer ";
        if !authorization.starts_with(bearer) {
            Err(Status::internal("Invalid auth header!"))?;
        }
        let auth = authorization[bearer.len()..].to_string();

        let handle = Uuid::from_str(auth.as_str())
            .map_err(|e| Status::internal(format!("Error locking contexts: {e}")))?;
        if let Some(context) = self.contexts.get(&handle) {
            Ok(context.clone())
        } else {
            Err(Status::internal(format!(
                "Context handle not found: {handle}"
            )))?
        }
    }

    async fn prepare_statement(
        query: &str,
        ctx: &Arc<SessionContext>,
    ) -> Result<LogicalPlan, Status> {
        let plan = ctx
            .sql(query)
            .await
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;
        Ok(plan)
    }

    async fn check_job(&self, job_id: &String) -> Result<Option<SuccessfulJob>, Status> {
        let status = self
            .server
            .state
            .task_manager
            .get_job_status(job_id)
            .await
            .map_err(|e| {
                let msg = format!("Error getting status for job {job_id}: {e:?}");
                error!("{}", msg);
                Status::internal(msg)
            })?;
        let status: JobStatus = match status {
            Some(status) => status,
            None => {
                let msg = format!("Error getting status for job {job_id}!");
                error!("{}", msg);
                Err(Status::internal(msg))?
            }
        };
        let status: job_status::Status = match status.status {
            Some(status) => status,
            None => {
                let msg = format!("Error getting status for job {job_id}!");
                error!("{}", msg);
                Err(Status::internal(msg))?
            }
        };
        match status {
            job_status::Status::Queued(_) => Ok(None),
            job_status::Status::Running(_) => Ok(None),
            job_status::Status::Failed(e) => {
                warn!("Error executing plan: {:?}", e);
                Err(Status::internal(format!(
                    "Error executing plan: {}",
                    e.error
                )))?
            }
            job_status::Status::Successful(comp) => Ok(Some(comp)),
        }
    }

    async fn job_to_fetch_part(
        &self,
        completed: SuccessfulJob,
        num_rows: &mut i64,
        num_bytes: &mut i64,
    ) -> Result<Vec<FlightEndpoint>, Status> {
        let mut fieps: Vec<_> = vec![];
        for loc in completed.partition_location.iter() {
            let (exec_host, exec_port) = if let Some(ref md) = loc.executor_meta {
                (md.host.clone(), md.port)
            } else {
                Err(Status::internal(
                    "Invalid partition location, missing executor metadata and advertise_endpoint flag is undefined.".to_string(),
                ))?
            };

            let (host, port) = match &self
                .server
                .state
                .config
                .advertise_flight_sql_endpoint
            {
                Some(endpoint) => {
                    let advertise_endpoint_vec: Vec<&str> = endpoint.split(':').collect();
                    match advertise_endpoint_vec.as_slice() {
                        [host_ip, port] => {
                            (String::from(*host_ip), FromStr::from_str(port).expect("Failed to parse port from advertise-endpoint."))
                        }
                        _ => {
                            Err(Status::internal("advertise-endpoint flag has incorrect format. Expected IP:Port".to_string()))?
                        }
                    }
                }
                None => (exec_host.clone(), exec_port),
            };

            let fetch = if let Some(ref id) = loc.partition_id {
                let fetch = protobuf::FetchPartition {
                    job_id: id.job_id.clone(),
                    stage_id: id.stage_id,
                    partition_id: id.partition_id,
                    path: loc.path.clone(),
                    // Use executor ip:port for routing to flight result
                    host: exec_host.clone(),
                    port: exec_port,
                };
                protobuf::Action {
                    action_type: Some(FetchPartition(fetch)),
                    settings: vec![],
                }
            } else {
                Err(Status::internal("Error getting partition ID".to_string()))?
            };
            if let Some(ref stats) = loc.partition_stats {
                *num_rows += stats.num_rows;
                *num_bytes += stats.num_bytes;
            } else {
                Err(Status::internal("Error getting stats".to_string()))?
            }
            let authority = format!("{}:{}", &host, &port);
            let loc = Location {
                uri: format!("grpc+tcp://{authority}"),
            };
            let buf = fetch.as_any().encode_to_vec();
            let ticket = Ticket { ticket: buf.into() };
            let fiep = FlightEndpoint {
                ticket: Some(ticket),
                location: vec![loc],
            };
            fieps.push(fiep);
        }
        Ok(fieps)
    }

    fn make_local_fieps(&self, job_id: &str) -> Result<Vec<FlightEndpoint>, Status> {
        let (host, port) = ("127.0.0.1".to_string(), 50050); // TODO: use advertise host
        let fetch = protobuf::FetchPartition {
            job_id: job_id.to_string(),
            stage_id: 0,
            partition_id: 0,
            path: job_id.to_string(),
            host: host.clone(),
            port,
        };
        let fetch = protobuf::Action {
            action_type: Some(FetchPartition(fetch)),
            settings: vec![],
        };
        let authority = format!("{}:{}", &host, &port); // TODO: use advertise host
        let loc = Location {
            uri: format!("grpc+tcp://{authority}"),
        };
        let buf = fetch.as_any().encode_to_vec();
        let ticket = Ticket { ticket: buf.into() };
        let fiep = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![loc],
        };
        let fieps = vec![fiep];
        Ok(fieps)
    }

    fn cache_plan(&self, plan: LogicalPlan) -> Result<Uuid, Status> {
        let handle = Uuid::new_v4();
        self.statements.insert(handle, plan);
        Ok(handle)
    }

    fn get_plan(&self, handle: &Uuid) -> Result<LogicalPlan, Status> {
        if let Some(plan) = self.statements.get(handle) {
            Ok(plan.clone())
        } else {
            Err(Status::internal(format!(
                "Statement handle not found: {handle}"
            )))?
        }
    }

    fn remove_plan(&self, handle: Uuid) -> Result<(), Status> {
        self.statements.remove(&handle);
        Ok(())
    }

    fn df_schema_to_arrow(&self, schema: &DFSchemaRef) -> Result<Vec<u8>, Status> {
        let arrow_schema: Schema = (&**schema).into();
        let schema_bytes = self.schema_to_arrow(Arc::new(arrow_schema))?;
        Ok(schema_bytes)
    }

    fn schema_to_arrow(&self, arrow_schema: SchemaRef) -> Result<Vec<u8>, Status> {
        let options = IpcWriteOptions::default();
        let pair = SchemaAsIpc::new(&arrow_schema, &options);
        let data_gen = IpcDataGenerator::default();
        let encoded_data = data_gen.schema_to_bytes(pair.0, pair.1);
        let mut schema_bytes = vec![];
        arrow::ipc::writer::write_message(&mut schema_bytes, encoded_data, pair.1)
            .map_err(|e| Status::internal(format!("Error encoding schema: {e}")))?;
        Ok(schema_bytes)
    }

    async fn enqueue_job(
        &self,
        ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
    ) -> Result<String, Status> {
        let job_id = self.server.state.task_manager.generate_job_id();
        let job_name = format!("Flight SQL job {job_id}");
        self.server
            .submit_job(&job_id, &job_name, ctx, plan)
            .await
            .map_err(|e| {
                let msg = format!("Failed to send JobQueued event for {job_id}: {e:?}");
                error!("{}", msg);
                Status::internal(msg)
            })?;
        Ok(job_id)
    }

    fn create_resp(
        schema_bytes: Vec<u8>,
        fieps: Vec<FlightEndpoint>,
        num_rows: i64,
        num_bytes: i64,
    ) -> Response<FlightInfo> {
        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Vec::new().into(),
            path: vec![],
        };
        let info = FlightInfo {
            schema: schema_bytes.into(),
            flight_descriptor: Some(flight_desc),
            endpoint: fieps,
            total_records: num_rows,
            total_bytes: num_bytes,
        };
        Response::new(info)
    }

    async fn execute_plan(
        &self,
        ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
    ) -> Result<Response<FlightInfo>, Status> {
        let job_id = self.enqueue_job(ctx, plan).await?;

        // poll for job completion
        let mut num_rows = 0;
        let mut num_bytes = 0;
        let fieps = loop {
            sleep(Duration::from_millis(100)).await;
            let completed = if let Some(comp) = self.check_job(&job_id).await? {
                comp
            } else {
                continue;
            };
            let fieps = self
                .job_to_fetch_part(completed, &mut num_rows, &mut num_bytes)
                .await?;
            break fieps;
        };

        // Generate response
        let schema_bytes = self.df_schema_to_arrow(plan.schema())?;
        let resp = Self::create_resp(schema_bytes, fieps, num_rows, num_bytes);
        Ok(resp)
    }

    async fn record_batch_to_resp(
        rb: RecordBatch,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        type FlightResult = Result<FlightData, Status>;
        let (tx, rx): (Sender<FlightResult>, Receiver<FlightResult>) = channel(2);
        let schema = (*rb.schema()).clone();
        let flights = batches_to_flight_data(schema, vec![rb])
            .map_err(|_| Status::internal("Error encoding batches".to_string()))?;
        for flight in flights {
            tx.send(Ok(flight))
                .await
                .map_err(|_| Status::internal("Error sending flight".to_string()))?;
        }
        let resp = Response::new(Box::pin(ReceiverStream::new(rx))
            as Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>);
        Ok(resp)
    }

    fn batch_to_schema_resp(
        &self,
        data: &RecordBatch,
        name: &str,
    ) -> Result<Response<FlightInfo>, Status> {
        let num_bytes = batch_byte_size(data) as i64;
        let schema = data.schema();
        let num_rows = data.num_rows() as i64;

        let fieps = self.make_local_fieps(name)?;
        let schema_bytes = self.schema_to_arrow(schema)?;
        let resp = Self::create_resp(schema_bytes, fieps, num_rows, num_bytes);
        Ok(resp)
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        debug!("do_handshake");
        for md in request.metadata().iter() {
            debug!("{:?}", md);
        }

        let basic = "Basic ";
        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::invalid_argument("authorization field not present"))?
            .to_str()
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        if !authorization.starts_with(basic) {
            Err(Status::invalid_argument(format!(
                "Auth type not implemented: {authorization}"
            )))?;
        }
        let base64 = &authorization[basic.len()..];
        let bytes = base64::decode(base64)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        let str = String::from_utf8(bytes)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        let parts: Vec<_> = str.split(':').collect();
        if parts.len() != 2 {
            Err(Status::invalid_argument("Invalid authorization header"))?;
        }
        let user = parts[0];
        let pass = parts[1];
        if user != "admin" || pass != "password" {
            Err(Status::unauthenticated("Invalid credentials!"))?
        }

        let token = self.create_ctx().await?;

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> =
            Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: arrow_flight::sql::Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_fallback type_url: {}", message.type_url);
        let ctx = self.get_ctx(&request)?;
        if !message.is::<protobuf::Action>() {
            Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )))?
        }

        let action: protobuf::Action = message
            .unpack()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .ok_or_else(|| Status::internal("Expected an Action but got None!"))?;
        let fp = match &action.action_type {
            Some(FetchPartition(fp)) => fp.clone(),
            None => Err(Status::internal("Expected an ActionType but got None!"))?,
        };

        // Well-known job ID: respond with the data
        match fp.job_id.as_str() {
            "get_flight_info_table_types" => {
                debug!("Responding with table types");
                let rb = FlightSqlServiceImpl::table_types().map_err(|_| {
                    Status::internal("Error getting table types".to_string())
                })?;
                let resp = Self::record_batch_to_resp(rb).await?;
                return Ok(resp);
            }
            "get_flight_info_tables" => {
                debug!("Responding with tables");
                let rb = self
                    .tables(ctx)
                    .map_err(|_| Status::internal("Error getting tables".to_string()))?;
                let resp = Self::record_batch_to_resp(rb).await?;
                return Ok(resp);
            }
            _ => {}
        }

        // Proxy the flight
        let addr = format!("http://{}:{}", fp.host, fp.port);
        debug!("Scheduler proxying flight for to {}", addr);
        let connection =
            create_grpc_client_connection(addr.clone(), Duration::from_secs(20))
                .await
                .map_err(|e| {
                    Status::internal(format!(
                    "Error connecting to Ballista scheduler or executor at {addr}: {e:?}"
                ))
                })?;
        let mut flight_client = FlightServiceClient::new(connection);
        let buf = action.encode_to_vec();
        let request = Request::new(Ticket { ticket: buf.into() });

        let stream = flight_client
            .do_get(request)
            .await
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .into_inner();
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_statement query:\n{}", query.query);

        let ctx = self.get_ctx(&request)?;
        let plan = Self::prepare_statement(&query.query, &ctx).await?;
        let resp = self.execute_plan(ctx, &plan).await?;

        debug!("Returning flight info...");
        Ok(resp)
    }

    async fn get_flight_info_prepared_statement(
        &self,
        handle: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_prepared_statement");
        let ctx = self.get_ctx(&request)?;
        let handle = Uuid::from_slice(handle.prepared_statement_handle.as_ref())
            .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?;
        let plan = self.get_plan(&handle)?;
        let resp = self.execute_plan(ctx, &plan).await?;

        debug!("Responding to query {}...", handle);
        Ok(resp)
    }

    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_catalogs");
        Err(Status::unimplemented("Implement get_flight_info_catalogs"))
    }
    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_schemas");
        Err(Status::unimplemented("Implement get_flight_info_schemas"))
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_tables");
        let ctx = self.get_ctx(&request)?;
        let data = self
            .tables(ctx)
            .map_err(|e| Status::internal(format!("Error getting tables: {e}")))?;
        let resp = self.batch_to_schema_resp(&data, "get_flight_info_tables")?;
        Ok(resp)
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_table_types");
        let data = FlightSqlServiceImpl::table_types()
            .map_err(|e| Status::internal(format!("Error getting table types: {e}")))?;
        let resp = self.batch_to_schema_resp(&data, "get_flight_info_table_types")?;
        Ok(resp)
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_sql_info");
        // TODO: implement for FlightSQL JDBC to work
        Err(Status::unimplemented("Implement CommandGetSqlInfo"))
    }
    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_primary_keys");
        Err(Status::unimplemented(
            "Implement get_flight_info_primary_keys",
        ))
    }
    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_exported_keys");
        Err(Status::unimplemented(
            "Implement get_flight_info_exported_keys",
        ))
    }
    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_imported_keys");
        Err(Status::unimplemented(
            "Implement get_flight_info_imported_keys",
        ))
    }
    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_cross_reference");
        Err(Status::unimplemented(
            "Implement get_flight_info_cross_reference",
        ))
    }

    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_statement");
        // let handle = Uuid::from_slice(&ticket.statement_handle)
        //     .map_err(|e| Status::internal(format!("Error decoding ticket: {}", e)))?;
        // let statements = self.statements.try_lock()
        //     .map_err(|e| Status::internal(format!("Error decoding ticket: {}", e)))?;
        // let plan = statements.get(&handle);
        Err(Status::unimplemented("Implement do_get_statement"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_prepared_statement");
        Err(Status::unimplemented("Implement do_get_prepared_statement"))
    }
    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_catalogs");
        Err(Status::unimplemented("Implement do_get_catalogs"))
    }
    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_schemas");
        Err(Status::unimplemented("Implement do_get_schemas"))
    }
    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_tables");
        Err(Status::unimplemented("Implement do_get_tables"))
    }
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_table_types");
        Err(Status::unimplemented("Implement do_get_table_types"))
    }
    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_sql_info");
        Err(Status::unimplemented("Implement do_get_sql_info"))
    }
    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_primary_keys");
        Err(Status::unimplemented("Implement do_get_primary_keys"))
    }
    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_exported_keys");
        Err(Status::unimplemented("Implement do_get_exported_keys"))
    }
    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_imported_keys");
        Err(Status::unimplemented("Implement do_get_imported_keys"))
    }
    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_cross_reference");
        Err(Status::unimplemented("Implement do_get_cross_reference"))
    }
    // do_put
    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        debug!("do_put_statement_update");
        Err(Status::unimplemented("Implement do_put_statement_update"))
    }
    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        debug!("do_put_prepared_statement_query");
        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_query",
        ))
    }
    async fn do_put_prepared_statement_update(
        &self,
        handle: CommandPreparedStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        debug!("do_put_prepared_statement_update");
        let ctx = self.get_ctx(&request)?;
        let handle = Uuid::from_slice(handle.prepared_statement_handle.as_ref())
            .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?;
        let plan = self.get_plan(&handle)?;
        let _ = self.execute_plan(ctx, &plan).await?;
        debug!("Sending -1 rows affected");
        Ok(-1)
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        debug!("do_action_create_prepared_statement");
        let ctx = self.get_ctx(&request)?;
        let plan = Self::prepare_statement(&query.query, &ctx).await?;
        let schema_bytes = self.df_schema_to_arrow(plan.schema())?;
        let handle = self.cache_plan(plan)?;
        debug!("Prepared statement {}:\n{}", handle, query.query);
        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.as_bytes().to_vec().into(),
            dataset_schema: schema_bytes.into(),
            parameter_schema: Vec::new().into(), // TODO: parameters
        };
        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        handle: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) {
        debug!("do_action_close_prepared_statement");
        let handle = Uuid::from_slice(handle.prepared_statement_handle.as_ref());
        let handle = if let Ok(handle) = handle {
            debug!("Closing {}", handle);
            handle
        } else {
            return;
        };
        let _ = self.remove_plan(handle);
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
