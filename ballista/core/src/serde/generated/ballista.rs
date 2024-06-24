/// /////////////////////////////////////////////////////////////////////////////////////////////////
/// Ballista Physical Plan
/// /////////////////////////////////////////////////////////////////////////////////////////////////
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BallistaPhysicalPlanNode {
    #[prost(oneof = "ballista_physical_plan_node::PhysicalPlanType", tags = "1, 2, 3")]
    pub physical_plan_type: ::core::option::Option<
        ballista_physical_plan_node::PhysicalPlanType,
    >,
}
/// Nested message and enum types in `BallistaPhysicalPlanNode`.
pub mod ballista_physical_plan_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PhysicalPlanType {
        #[prost(message, tag = "1")]
        ShuffleWriter(super::ShuffleWriterExecNode),
        #[prost(message, tag = "2")]
        ShuffleReader(super::ShuffleReaderExecNode),
        #[prost(message, tag = "3")]
        UnresolvedShuffle(super::UnresolvedShuffleExecNode),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleWriterExecNode {
    /// TODO it seems redundant to provide job and stage id here since we also have them
    /// in the TaskDefinition that wraps this plan
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub stage_id: u32,
    #[prost(message, optional, tag = "3")]
    pub input: ::core::option::Option<::datafusion_proto::protobuf::PhysicalPlanNode>,
    #[prost(message, optional, tag = "4")]
    pub output_partitioning: ::core::option::Option<
        ::datafusion_proto::protobuf::PhysicalHashRepartition,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnresolvedShuffleExecNode {
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    #[prost(message, optional, tag = "2")]
    pub schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
    #[prost(uint32, tag = "4")]
    pub output_partition_count: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleReaderExecNode {
    #[prost(message, repeated, tag = "1")]
    pub partition: ::prost::alloc::vec::Vec<ShuffleReaderPartition>,
    #[prost(message, optional, tag = "2")]
    pub schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
    /// The stage to read from
    #[prost(uint32, tag = "3")]
    pub stage_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleReaderPartition {
    /// each partition of a shuffle read can read data from multiple locations
    #[prost(message, repeated, tag = "1")]
    pub location: ::prost::alloc::vec::Vec<PartitionLocation>,
}
/// /////////////////////////////////////////////////////////////////////////////////////////////////
/// Ballista Scheduling
/// /////////////////////////////////////////////////////////////////////////////////////////////////
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionGraph {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub session_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub status: ::core::option::Option<JobStatus>,
    #[prost(message, repeated, tag = "4")]
    pub stages: ::prost::alloc::vec::Vec<ExecutionGraphStage>,
    #[prost(uint64, tag = "5")]
    pub output_partitions: u64,
    #[prost(message, repeated, tag = "6")]
    pub output_locations: ::prost::alloc::vec::Vec<PartitionLocation>,
    #[prost(string, tag = "7")]
    pub scheduler_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "8")]
    pub task_id_gen: u32,
    #[prost(message, repeated, tag = "9")]
    pub failed_attempts: ::prost::alloc::vec::Vec<StageAttempts>,
    #[prost(string, tag = "10")]
    pub job_name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "11")]
    pub start_time: u64,
    #[prost(uint64, tag = "12")]
    pub end_time: u64,
    #[prost(uint64, tag = "13")]
    pub queued_at: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageAttempts {
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    #[prost(uint32, repeated, tag = "2")]
    pub stage_attempt_num: ::prost::alloc::vec::Vec<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionGraphStage {
    #[prost(oneof = "execution_graph_stage::StageType", tags = "1, 2, 3, 4")]
    pub stage_type: ::core::option::Option<execution_graph_stage::StageType>,
}
/// Nested message and enum types in `ExecutionGraphStage`.
pub mod execution_graph_stage {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum StageType {
        #[prost(message, tag = "1")]
        UnresolvedStage(super::UnResolvedStage),
        #[prost(message, tag = "2")]
        ResolvedStage(super::ResolvedStage),
        #[prost(message, tag = "3")]
        SuccessfulStage(super::SuccessfulStage),
        #[prost(message, tag = "4")]
        FailedStage(super::FailedStage),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnResolvedStage {
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    #[prost(uint32, repeated, tag = "3")]
    pub output_links: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, repeated, tag = "4")]
    pub inputs: ::prost::alloc::vec::Vec<GraphStageInput>,
    #[prost(bytes = "vec", tag = "5")]
    pub plan: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag = "6")]
    pub stage_attempt_num: u32,
    #[prost(string, repeated, tag = "7")]
    pub last_attempt_failure_reasons: ::prost::alloc::vec::Vec<
        ::prost::alloc::string::String,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedStage {
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    #[prost(uint32, tag = "2")]
    pub partitions: u32,
    #[prost(uint32, repeated, tag = "4")]
    pub output_links: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, repeated, tag = "5")]
    pub inputs: ::prost::alloc::vec::Vec<GraphStageInput>,
    #[prost(bytes = "vec", tag = "6")]
    pub plan: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag = "7")]
    pub stage_attempt_num: u32,
    #[prost(string, repeated, tag = "8")]
    pub last_attempt_failure_reasons: ::prost::alloc::vec::Vec<
        ::prost::alloc::string::String,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuccessfulStage {
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    #[prost(uint32, tag = "2")]
    pub partitions: u32,
    #[prost(uint32, repeated, tag = "4")]
    pub output_links: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, repeated, tag = "5")]
    pub inputs: ::prost::alloc::vec::Vec<GraphStageInput>,
    #[prost(bytes = "vec", tag = "6")]
    pub plan: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "7")]
    pub task_infos: ::prost::alloc::vec::Vec<TaskInfo>,
    #[prost(message, repeated, tag = "8")]
    pub stage_metrics: ::prost::alloc::vec::Vec<OperatorMetricsSet>,
    #[prost(uint32, tag = "9")]
    pub stage_attempt_num: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FailedStage {
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    #[prost(uint32, tag = "2")]
    pub partitions: u32,
    #[prost(uint32, repeated, tag = "4")]
    pub output_links: ::prost::alloc::vec::Vec<u32>,
    #[prost(bytes = "vec", tag = "5")]
    pub plan: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "6")]
    pub task_infos: ::prost::alloc::vec::Vec<TaskInfo>,
    #[prost(message, repeated, tag = "7")]
    pub stage_metrics: ::prost::alloc::vec::Vec<OperatorMetricsSet>,
    #[prost(string, tag = "8")]
    pub error_message: ::prost::alloc::string::String,
    #[prost(uint32, tag = "9")]
    pub stage_attempt_num: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskInfo {
    #[prost(uint32, tag = "1")]
    pub task_id: u32,
    #[prost(uint32, tag = "2")]
    pub partition_id: u32,
    /// Scheduler schedule time
    #[prost(uint64, tag = "3")]
    pub scheduled_time: u64,
    /// Scheduler launch time
    #[prost(uint64, tag = "4")]
    pub launch_time: u64,
    /// The time the Executor start to run the task
    #[prost(uint64, tag = "5")]
    pub start_exec_time: u64,
    /// The time the Executor finish the task
    #[prost(uint64, tag = "6")]
    pub end_exec_time: u64,
    /// Scheduler side finish time
    #[prost(uint64, tag = "7")]
    pub finish_time: u64,
    #[prost(oneof = "task_info::Status", tags = "8, 9, 10")]
    pub status: ::core::option::Option<task_info::Status>,
}
/// Nested message and enum types in `TaskInfo`.
pub mod task_info {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Status {
        #[prost(message, tag = "8")]
        Running(super::RunningTask),
        #[prost(message, tag = "9")]
        Failed(super::FailedTask),
        #[prost(message, tag = "10")]
        Successful(super::SuccessfulTask),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GraphStageInput {
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    #[prost(message, repeated, tag = "2")]
    pub partition_locations: ::prost::alloc::vec::Vec<TaskInputPartitions>,
    #[prost(bool, tag = "3")]
    pub complete: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskInputPartitions {
    #[prost(uint32, tag = "1")]
    pub partition: u32,
    #[prost(message, repeated, tag = "2")]
    pub partition_location: ::prost::alloc::vec::Vec<PartitionLocation>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValuePair {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Action {
    /// configuration settings
    #[prost(message, repeated, tag = "100")]
    pub settings: ::prost::alloc::vec::Vec<KeyValuePair>,
    #[prost(oneof = "action::ActionType", tags = "3")]
    pub action_type: ::core::option::Option<action::ActionType>,
}
/// Nested message and enum types in `Action`.
pub mod action {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ActionType {
        /// Fetch a partition from an executor
        #[prost(message, tag = "3")]
        FetchPartition(super::FetchPartition),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutePartition {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub stage_id: u32,
    #[prost(uint32, repeated, tag = "3")]
    pub partition_id: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, optional, tag = "4")]
    pub plan: ::core::option::Option<::datafusion_proto::protobuf::PhysicalPlanNode>,
    /// The task could need to read partitions from other executors
    #[prost(message, repeated, tag = "5")]
    pub partition_location: ::prost::alloc::vec::Vec<PartitionLocation>,
    /// Output partition for shuffle writer
    #[prost(message, optional, tag = "6")]
    pub output_partitioning: ::core::option::Option<
        ::datafusion_proto::protobuf::PhysicalHashRepartition,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchPartition {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub stage_id: u32,
    #[prost(uint32, tag = "3")]
    pub partition_id: u32,
    #[prost(string, tag = "4")]
    pub path: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag = "6")]
    pub port: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionLocation {
    /// partition_id of the map stage who produces the shuffle.
    #[prost(uint32, tag = "1")]
    pub map_partition_id: u32,
    /// partition_id of the shuffle, a composition of(job_id + map_stage_id + partition_id).
    #[prost(message, optional, tag = "2")]
    pub partition_id: ::core::option::Option<PartitionId>,
    #[prost(message, optional, tag = "3")]
    pub executor_meta: ::core::option::Option<ExecutorMetadata>,
    #[prost(message, optional, tag = "4")]
    pub partition_stats: ::core::option::Option<PartitionStats>,
    #[prost(string, tag = "5")]
    pub path: ::prost::alloc::string::String,
}
/// Unique identifier for a materialized partition of data
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionId {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub stage_id: u32,
    #[prost(uint32, tag = "4")]
    pub partition_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskId {
    #[prost(uint32, tag = "1")]
    pub task_id: u32,
    #[prost(uint32, tag = "2")]
    pub task_attempt_num: u32,
    #[prost(uint32, tag = "3")]
    pub partition_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionStats {
    #[prost(int64, tag = "1")]
    pub num_rows: i64,
    #[prost(int64, tag = "2")]
    pub num_batches: i64,
    #[prost(int64, tag = "3")]
    pub num_bytes: i64,
    #[prost(message, repeated, tag = "4")]
    pub column_stats: ::prost::alloc::vec::Vec<ColumnStats>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnStats {
    #[prost(message, optional, tag = "1")]
    pub min_value: ::core::option::Option<::datafusion_proto::protobuf::ScalarValue>,
    #[prost(message, optional, tag = "2")]
    pub max_value: ::core::option::Option<::datafusion_proto::protobuf::ScalarValue>,
    #[prost(uint32, tag = "3")]
    pub null_count: u32,
    #[prost(uint32, tag = "4")]
    pub distinct_count: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OperatorMetricsSet {
    #[prost(message, repeated, tag = "1")]
    pub metrics: ::prost::alloc::vec::Vec<OperatorMetric>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NamedCount {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub value: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NamedGauge {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub value: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NamedTime {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub value: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OperatorMetric {
    #[prost(oneof = "operator_metric::Metric", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10")]
    pub metric: ::core::option::Option<operator_metric::Metric>,
}
/// Nested message and enum types in `OperatorMetric`.
pub mod operator_metric {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Metric {
        #[prost(uint64, tag = "1")]
        OutputRows(u64),
        #[prost(uint64, tag = "2")]
        ElapseTime(u64),
        #[prost(uint64, tag = "3")]
        SpillCount(u64),
        #[prost(uint64, tag = "4")]
        SpilledBytes(u64),
        #[prost(uint64, tag = "5")]
        CurrentMemoryUsage(u64),
        #[prost(message, tag = "6")]
        Count(super::NamedCount),
        #[prost(message, tag = "7")]
        Gauge(super::NamedGauge),
        #[prost(message, tag = "8")]
        Time(super::NamedTime),
        #[prost(int64, tag = "9")]
        StartTimestamp(i64),
        #[prost(int64, tag = "10")]
        EndTimestamp(i64),
    }
}
/// Used by scheduler
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorMetadata {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub port: u32,
    #[prost(uint32, tag = "4")]
    pub grpc_port: u32,
    #[prost(message, optional, tag = "5")]
    pub specification: ::core::option::Option<ExecutorSpecification>,
}
/// Used by grpc
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorRegistration {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub port: u32,
    #[prost(uint32, tag = "4")]
    pub grpc_port: u32,
    #[prost(message, optional, tag = "5")]
    pub specification: ::core::option::Option<ExecutorSpecification>,
    /// "optional" keyword is stable in protoc 3.15 but prost is still on 3.14 (see <https://github.com/tokio-rs/prost/issues/430> and <https://github.com/tokio-rs/prost/pull/455>)
    /// this syntax is ugly but is binary compatible with the "optional" keyword (see <https://stackoverflow.com/questions/42622015/how-to-define-an-optional-field-in-protobuf-3>)
    #[prost(oneof = "executor_registration::OptionalHost", tags = "2")]
    pub optional_host: ::core::option::Option<executor_registration::OptionalHost>,
}
/// Nested message and enum types in `ExecutorRegistration`.
pub mod executor_registration {
    /// "optional" keyword is stable in protoc 3.15 but prost is still on 3.14 (see <https://github.com/tokio-rs/prost/issues/430> and <https://github.com/tokio-rs/prost/pull/455>)
    /// this syntax is ugly but is binary compatible with the "optional" keyword (see <https://stackoverflow.com/questions/42622015/how-to-define-an-optional-field-in-protobuf-3>)
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum OptionalHost {
        #[prost(string, tag = "2")]
        Host(::prost::alloc::string::String),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorHeartbeat {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    /// Unix epoch-based timestamp in seconds
    #[prost(uint64, tag = "2")]
    pub timestamp: u64,
    #[prost(message, repeated, tag = "3")]
    pub metrics: ::prost::alloc::vec::Vec<ExecutorMetric>,
    #[prost(message, optional, tag = "4")]
    pub status: ::core::option::Option<ExecutorStatus>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorMetric {
    /// TODO add more metrics
    #[prost(oneof = "executor_metric::Metric", tags = "1")]
    pub metric: ::core::option::Option<executor_metric::Metric>,
}
/// Nested message and enum types in `ExecutorMetric`.
pub mod executor_metric {
    /// TODO add more metrics
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Metric {
        #[prost(uint64, tag = "1")]
        AvailableMemory(u64),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorStatus {
    #[prost(oneof = "executor_status::Status", tags = "1, 2, 3, 4")]
    pub status: ::core::option::Option<executor_status::Status>,
}
/// Nested message and enum types in `ExecutorStatus`.
pub mod executor_status {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Status {
        #[prost(string, tag = "1")]
        Active(::prost::alloc::string::String),
        #[prost(string, tag = "2")]
        Dead(::prost::alloc::string::String),
        #[prost(string, tag = "3")]
        Unknown(::prost::alloc::string::String),
        #[prost(string, tag = "4")]
        Terminating(::prost::alloc::string::String),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorSpecification {
    #[prost(message, repeated, tag = "1")]
    pub resources: ::prost::alloc::vec::Vec<ExecutorResource>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorResource {
    /// TODO add more resources
    #[prost(oneof = "executor_resource::Resource", tags = "1")]
    pub resource: ::core::option::Option<executor_resource::Resource>,
}
/// Nested message and enum types in `ExecutorResource`.
pub mod executor_resource {
    /// TODO add more resources
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Resource {
        #[prost(uint32, tag = "1")]
        TaskSlots(u32),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AvailableTaskSlots {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub slots: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorTaskSlots {
    #[prost(message, repeated, tag = "1")]
    pub task_slots: ::prost::alloc::vec::Vec<AvailableTaskSlots>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorData {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub resources: ::prost::alloc::vec::Vec<ExecutorResourcePair>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorResourcePair {
    #[prost(message, optional, tag = "1")]
    pub total: ::core::option::Option<ExecutorResource>,
    #[prost(message, optional, tag = "2")]
    pub available: ::core::option::Option<ExecutorResource>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RunningTask {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FailedTask {
    #[prost(string, tag = "1")]
    pub error: ::prost::alloc::string::String,
    #[prost(bool, tag = "2")]
    pub retryable: bool,
    /// Whether this task failure should be counted to the maximum number of times the task is allowed to retry
    #[prost(bool, tag = "3")]
    pub count_to_failures: bool,
    #[prost(oneof = "failed_task::FailedReason", tags = "4, 5, 6, 7, 8, 9")]
    pub failed_reason: ::core::option::Option<failed_task::FailedReason>,
}
/// Nested message and enum types in `FailedTask`.
pub mod failed_task {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FailedReason {
        #[prost(message, tag = "4")]
        ExecutionError(super::ExecutionError),
        #[prost(message, tag = "5")]
        FetchPartitionError(super::FetchPartitionError),
        #[prost(message, tag = "6")]
        IoError(super::IoError),
        #[prost(message, tag = "7")]
        ExecutorLost(super::ExecutorLost),
        /// A successful task's result is lost due to executor lost
        #[prost(message, tag = "8")]
        ResultLost(super::ResultLost),
        #[prost(message, tag = "9")]
        TaskKilled(super::TaskKilled),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuccessfulTask {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    /// TODO tasks are currently always shuffle writes but this will not always be the case
    /// so we might want to think about some refactoring of the task definitions
    #[prost(message, repeated, tag = "2")]
    pub partitions: ::prost::alloc::vec::Vec<ShuffleWritePartition>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionError {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchPartitionError {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub map_stage_id: u32,
    #[prost(uint32, tag = "3")]
    pub map_partition_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IoError {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorLost {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResultLost {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskKilled {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleWritePartition {
    #[prost(uint64, tag = "1")]
    pub partition_id: u64,
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub num_batches: u64,
    #[prost(uint64, tag = "4")]
    pub num_rows: u64,
    #[prost(uint64, tag = "5")]
    pub num_bytes: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskStatus {
    #[prost(uint32, tag = "1")]
    pub task_id: u32,
    #[prost(string, tag = "2")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub stage_id: u32,
    #[prost(uint32, tag = "4")]
    pub stage_attempt_num: u32,
    #[prost(uint32, tag = "5")]
    pub partition_id: u32,
    #[prost(uint64, tag = "6")]
    pub launch_time: u64,
    #[prost(uint64, tag = "7")]
    pub start_exec_time: u64,
    #[prost(uint64, tag = "8")]
    pub end_exec_time: u64,
    #[prost(message, repeated, tag = "12")]
    pub metrics: ::prost::alloc::vec::Vec<OperatorMetricsSet>,
    #[prost(oneof = "task_status::Status", tags = "9, 10, 11")]
    pub status: ::core::option::Option<task_status::Status>,
}
/// Nested message and enum types in `TaskStatus`.
pub mod task_status {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Status {
        #[prost(message, tag = "9")]
        Running(super::RunningTask),
        #[prost(message, tag = "10")]
        Failed(super::FailedTask),
        #[prost(message, tag = "11")]
        Successful(super::SuccessfulTask),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PollWorkParams {
    #[prost(message, optional, tag = "1")]
    pub metadata: ::core::option::Option<ExecutorRegistration>,
    #[prost(uint32, tag = "2")]
    pub num_free_slots: u32,
    /// All tasks must be reported until they reach the failed or completed state
    #[prost(message, repeated, tag = "3")]
    pub task_status: ::prost::alloc::vec::Vec<TaskStatus>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskDefinition {
    #[prost(uint32, tag = "1")]
    pub task_id: u32,
    #[prost(uint32, tag = "2")]
    pub task_attempt_num: u32,
    #[prost(string, tag = "3")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "4")]
    pub stage_id: u32,
    #[prost(uint32, tag = "5")]
    pub stage_attempt_num: u32,
    #[prost(uint32, tag = "6")]
    pub partition_id: u32,
    #[prost(bytes = "vec", tag = "7")]
    pub plan: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, tag = "9")]
    pub session_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "10")]
    pub launch_time: u64,
    #[prost(message, repeated, tag = "11")]
    pub props: ::prost::alloc::vec::Vec<KeyValuePair>,
}
/// A set of tasks in the same stage
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiTaskDefinition {
    #[prost(message, repeated, tag = "1")]
    pub task_ids: ::prost::alloc::vec::Vec<TaskId>,
    #[prost(string, tag = "2")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub stage_id: u32,
    #[prost(uint32, tag = "4")]
    pub stage_attempt_num: u32,
    #[prost(bytes = "vec", tag = "5")]
    pub plan: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, tag = "7")]
    pub session_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "8")]
    pub launch_time: u64,
    #[prost(message, repeated, tag = "9")]
    pub props: ::prost::alloc::vec::Vec<KeyValuePair>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SessionSettings {
    #[prost(message, repeated, tag = "1")]
    pub configs: ::prost::alloc::vec::Vec<KeyValuePair>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JobSessionConfig {
    #[prost(string, tag = "1")]
    pub session_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub configs: ::prost::alloc::vec::Vec<KeyValuePair>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PollWorkResult {
    #[prost(message, repeated, tag = "1")]
    pub tasks: ::prost::alloc::vec::Vec<TaskDefinition>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterExecutorParams {
    #[prost(message, optional, tag = "1")]
    pub metadata: ::core::option::Option<ExecutorRegistration>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterExecutorResult {
    #[prost(bool, tag = "1")]
    pub success: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartBeatParams {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub metrics: ::prost::alloc::vec::Vec<ExecutorMetric>,
    #[prost(message, optional, tag = "3")]
    pub status: ::core::option::Option<ExecutorStatus>,
    #[prost(message, optional, tag = "4")]
    pub metadata: ::core::option::Option<ExecutorRegistration>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartBeatResult {
    /// TODO it's from Spark for BlockManager
    #[prost(bool, tag = "1")]
    pub reregister: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StopExecutorParams {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    /// stop reason
    #[prost(string, tag = "2")]
    pub reason: ::prost::alloc::string::String,
    /// force to stop the executor immediately
    #[prost(bool, tag = "3")]
    pub force: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StopExecutorResult {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorStoppedParams {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    /// stop reason
    #[prost(string, tag = "2")]
    pub reason: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutorStoppedResult {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateTaskStatusParams {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    /// All tasks must be reported until they reach the failed or completed state
    #[prost(message, repeated, tag = "2")]
    pub task_status: ::prost::alloc::vec::Vec<TaskStatus>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateTaskStatusResult {
    #[prost(bool, tag = "1")]
    pub success: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteQueryParams {
    #[prost(message, repeated, tag = "4")]
    pub settings: ::prost::alloc::vec::Vec<KeyValuePair>,
    #[prost(oneof = "execute_query_params::Query", tags = "1, 2")]
    pub query: ::core::option::Option<execute_query_params::Query>,
    #[prost(oneof = "execute_query_params::OptionalSessionId", tags = "3")]
    pub optional_session_id: ::core::option::Option<
        execute_query_params::OptionalSessionId,
    >,
}
/// Nested message and enum types in `ExecuteQueryParams`.
pub mod execute_query_params {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Query {
        #[prost(bytes, tag = "1")]
        LogicalPlan(::prost::alloc::vec::Vec<u8>),
        #[prost(string, tag = "2")]
        Sql(::prost::alloc::string::String),
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum OptionalSessionId {
        #[prost(string, tag = "3")]
        SessionId(::prost::alloc::string::String),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSessionParams {
    #[prost(message, repeated, tag = "1")]
    pub settings: ::prost::alloc::vec::Vec<KeyValuePair>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSessionResult {
    #[prost(string, tag = "1")]
    pub session_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSessionParams {
    #[prost(string, tag = "1")]
    pub session_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub settings: ::prost::alloc::vec::Vec<KeyValuePair>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSessionResult {
    #[prost(bool, tag = "1")]
    pub success: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveSessionParams {
    #[prost(string, tag = "1")]
    pub session_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveSessionResult {
    #[prost(bool, tag = "1")]
    pub success: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteSqlParams {
    #[prost(string, tag = "1")]
    pub sql: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteQueryResult {
    #[prost(oneof = "execute_query_result::Result", tags = "1, 2")]
    pub result: ::core::option::Option<execute_query_result::Result>,
}
/// Nested message and enum types in `ExecuteQueryResult`.
pub mod execute_query_result {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "1")]
        Success(super::ExecuteQuerySuccessResult),
        #[prost(message, tag = "2")]
        Failure(super::ExecuteQueryFailureResult),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteQuerySuccessResult {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub session_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteQueryFailureResult {
    #[prost(oneof = "execute_query_failure_result::Failure", tags = "1, 2, 3")]
    pub failure: ::core::option::Option<execute_query_failure_result::Failure>,
}
/// Nested message and enum types in `ExecuteQueryFailureResult`.
pub mod execute_query_failure_result {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Failure {
        #[prost(string, tag = "1")]
        SessionNotFound(::prost::alloc::string::String),
        #[prost(string, tag = "2")]
        PlanParsingFailure(::prost::alloc::string::String),
        #[prost(string, tag = "3")]
        SqlParsingFailure(::prost::alloc::string::String),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetJobStatusParams {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuccessfulJob {
    #[prost(message, repeated, tag = "1")]
    pub partition_location: ::prost::alloc::vec::Vec<PartitionLocation>,
    #[prost(uint64, tag = "2")]
    pub queued_at: u64,
    #[prost(uint64, tag = "3")]
    pub started_at: u64,
    #[prost(uint64, tag = "4")]
    pub ended_at: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueuedJob {
    #[prost(uint64, tag = "1")]
    pub queued_at: u64,
}
/// TODO: add progress report
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RunningJob {
    #[prost(uint64, tag = "1")]
    pub queued_at: u64,
    #[prost(uint64, tag = "2")]
    pub started_at: u64,
    #[prost(string, tag = "3")]
    pub scheduler: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FailedJob {
    #[prost(string, tag = "1")]
    pub error: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub queued_at: u64,
    #[prost(uint64, tag = "3")]
    pub started_at: u64,
    #[prost(uint64, tag = "4")]
    pub ended_at: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JobStatus {
    #[prost(string, tag = "5")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub job_name: ::prost::alloc::string::String,
    #[prost(oneof = "job_status::Status", tags = "1, 2, 3, 4")]
    pub status: ::core::option::Option<job_status::Status>,
}
/// Nested message and enum types in `JobStatus`.
pub mod job_status {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Status {
        #[prost(message, tag = "1")]
        Queued(super::QueuedJob),
        #[prost(message, tag = "2")]
        Running(super::RunningJob),
        #[prost(message, tag = "3")]
        Failed(super::FailedJob),
        #[prost(message, tag = "4")]
        Successful(super::SuccessfulJob),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetJobStatusResult {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<JobStatus>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFileMetadataParams {
    #[prost(string, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub file_type: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFileMetadataResult {
    #[prost(message, optional, tag = "1")]
    pub schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilePartitionMetadata {
    #[prost(string, repeated, tag = "1")]
    pub filename: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelJobParams {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelJobResult {
    #[prost(bool, tag = "1")]
    pub cancelled: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CleanJobDataParams {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CleanJobDataResult {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LaunchTaskParams {
    /// Allow to launch a task set to an executor at once
    #[prost(message, repeated, tag = "1")]
    pub tasks: ::prost::alloc::vec::Vec<TaskDefinition>,
    #[prost(string, tag = "2")]
    pub scheduler_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LaunchMultiTaskParams {
    /// Allow to launch a task set to an executor at once
    #[prost(message, repeated, tag = "1")]
    pub multi_tasks: ::prost::alloc::vec::Vec<MultiTaskDefinition>,
    #[prost(string, tag = "2")]
    pub scheduler_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LaunchTaskResult {
    /// TODO when part of the task set are scheduled successfully
    #[prost(bool, tag = "1")]
    pub success: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LaunchMultiTaskResult {
    /// TODO when part of the task set are scheduled successfully
    #[prost(bool, tag = "1")]
    pub success: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelTasksParams {
    #[prost(message, repeated, tag = "1")]
    pub task_infos: ::prost::alloc::vec::Vec<RunningTaskInfo>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelTasksResult {
    #[prost(bool, tag = "1")]
    pub cancelled: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveJobDataParams {
    #[prost(string, tag = "1")]
    pub job_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveJobDataResult {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RunningTaskInfo {
    #[prost(uint32, tag = "1")]
    pub task_id: u32,
    #[prost(string, tag = "2")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub stage_id: u32,
    #[prost(uint32, tag = "4")]
    pub partition_id: u32,
}
/// Generated client implementations.
pub mod scheduler_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct SchedulerGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl SchedulerGrpcClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> SchedulerGrpcClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> SchedulerGrpcClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            SchedulerGrpcClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// Executors must poll the scheduler for heartbeat and to receive tasks
        pub async fn poll_work(
            &mut self,
            request: impl tonic::IntoRequest<super::PollWorkParams>,
        ) -> std::result::Result<tonic::Response<super::PollWorkResult>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/PollWork",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("ballista.protobuf.SchedulerGrpc", "PollWork"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn register_executor(
            &mut self,
            request: impl tonic::IntoRequest<super::RegisterExecutorParams>,
        ) -> std::result::Result<
            tonic::Response<super::RegisterExecutorResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/RegisterExecutor",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "ballista.protobuf.SchedulerGrpc",
                        "RegisterExecutor",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Push-based task scheduler will only leverage this interface
        /// rather than the PollWork interface to report executor states
        pub async fn heart_beat_from_executor(
            &mut self,
            request: impl tonic::IntoRequest<super::HeartBeatParams>,
        ) -> std::result::Result<
            tonic::Response<super::HeartBeatResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/HeartBeatFromExecutor",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "ballista.protobuf.SchedulerGrpc",
                        "HeartBeatFromExecutor",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn update_task_status(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateTaskStatusParams>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateTaskStatusResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/UpdateTaskStatus",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "ballista.protobuf.SchedulerGrpc",
                        "UpdateTaskStatus",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_file_metadata(
            &mut self,
            request: impl tonic::IntoRequest<super::GetFileMetadataParams>,
        ) -> std::result::Result<
            tonic::Response<super::GetFileMetadataResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/GetFileMetadata",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.SchedulerGrpc", "GetFileMetadata"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn create_session(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateSessionParams>,
        ) -> std::result::Result<
            tonic::Response<super::CreateSessionResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/CreateSession",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.SchedulerGrpc", "CreateSession"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn update_session(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateSessionParams>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateSessionResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/UpdateSession",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.SchedulerGrpc", "UpdateSession"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn remove_session(
            &mut self,
            request: impl tonic::IntoRequest<super::RemoveSessionParams>,
        ) -> std::result::Result<
            tonic::Response<super::RemoveSessionResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/RemoveSession",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.SchedulerGrpc", "RemoveSession"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn execute_query(
            &mut self,
            request: impl tonic::IntoRequest<super::ExecuteQueryParams>,
        ) -> std::result::Result<
            tonic::Response<super::ExecuteQueryResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/ExecuteQuery",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.SchedulerGrpc", "ExecuteQuery"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_job_status(
            &mut self,
            request: impl tonic::IntoRequest<super::GetJobStatusParams>,
        ) -> std::result::Result<
            tonic::Response<super::GetJobStatusResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/GetJobStatus",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.SchedulerGrpc", "GetJobStatus"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Used by Executor to tell Scheduler it is stopped.
        pub async fn executor_stopped(
            &mut self,
            request: impl tonic::IntoRequest<super::ExecutorStoppedParams>,
        ) -> std::result::Result<
            tonic::Response<super::ExecutorStoppedResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/ExecutorStopped",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.SchedulerGrpc", "ExecutorStopped"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn cancel_job(
            &mut self,
            request: impl tonic::IntoRequest<super::CancelJobParams>,
        ) -> std::result::Result<
            tonic::Response<super::CancelJobResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/CancelJob",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("ballista.protobuf.SchedulerGrpc", "CancelJob"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn clean_job_data(
            &mut self,
            request: impl tonic::IntoRequest<super::CleanJobDataParams>,
        ) -> std::result::Result<
            tonic::Response<super::CleanJobDataResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.SchedulerGrpc/CleanJobData",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.SchedulerGrpc", "CleanJobData"),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod executor_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ExecutorGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ExecutorGrpcClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ExecutorGrpcClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ExecutorGrpcClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ExecutorGrpcClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn launch_task(
            &mut self,
            request: impl tonic::IntoRequest<super::LaunchTaskParams>,
        ) -> std::result::Result<
            tonic::Response<super::LaunchTaskResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.ExecutorGrpc/LaunchTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("ballista.protobuf.ExecutorGrpc", "LaunchTask"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn launch_multi_task(
            &mut self,
            request: impl tonic::IntoRequest<super::LaunchMultiTaskParams>,
        ) -> std::result::Result<
            tonic::Response<super::LaunchMultiTaskResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.ExecutorGrpc/LaunchMultiTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.ExecutorGrpc", "LaunchMultiTask"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn stop_executor(
            &mut self,
            request: impl tonic::IntoRequest<super::StopExecutorParams>,
        ) -> std::result::Result<
            tonic::Response<super::StopExecutorResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.ExecutorGrpc/StopExecutor",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.ExecutorGrpc", "StopExecutor"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn cancel_tasks(
            &mut self,
            request: impl tonic::IntoRequest<super::CancelTasksParams>,
        ) -> std::result::Result<
            tonic::Response<super::CancelTasksResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.ExecutorGrpc/CancelTasks",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.ExecutorGrpc", "CancelTasks"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn remove_job_data(
            &mut self,
            request: impl tonic::IntoRequest<super::RemoveJobDataParams>,
        ) -> std::result::Result<
            tonic::Response<super::RemoveJobDataResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ballista.protobuf.ExecutorGrpc/RemoveJobData",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("ballista.protobuf.ExecutorGrpc", "RemoveJobData"),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod scheduler_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with SchedulerGrpcServer.
    #[async_trait]
    pub trait SchedulerGrpc: Send + Sync + 'static {
        /// Executors must poll the scheduler for heartbeat and to receive tasks
        async fn poll_work(
            &self,
            request: tonic::Request<super::PollWorkParams>,
        ) -> std::result::Result<tonic::Response<super::PollWorkResult>, tonic::Status>;
        async fn register_executor(
            &self,
            request: tonic::Request<super::RegisterExecutorParams>,
        ) -> std::result::Result<
            tonic::Response<super::RegisterExecutorResult>,
            tonic::Status,
        >;
        /// Push-based task scheduler will only leverage this interface
        /// rather than the PollWork interface to report executor states
        async fn heart_beat_from_executor(
            &self,
            request: tonic::Request<super::HeartBeatParams>,
        ) -> std::result::Result<tonic::Response<super::HeartBeatResult>, tonic::Status>;
        async fn update_task_status(
            &self,
            request: tonic::Request<super::UpdateTaskStatusParams>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateTaskStatusResult>,
            tonic::Status,
        >;
        async fn get_file_metadata(
            &self,
            request: tonic::Request<super::GetFileMetadataParams>,
        ) -> std::result::Result<
            tonic::Response<super::GetFileMetadataResult>,
            tonic::Status,
        >;
        async fn create_session(
            &self,
            request: tonic::Request<super::CreateSessionParams>,
        ) -> std::result::Result<
            tonic::Response<super::CreateSessionResult>,
            tonic::Status,
        >;
        async fn update_session(
            &self,
            request: tonic::Request<super::UpdateSessionParams>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateSessionResult>,
            tonic::Status,
        >;
        async fn remove_session(
            &self,
            request: tonic::Request<super::RemoveSessionParams>,
        ) -> std::result::Result<
            tonic::Response<super::RemoveSessionResult>,
            tonic::Status,
        >;
        async fn execute_query(
            &self,
            request: tonic::Request<super::ExecuteQueryParams>,
        ) -> std::result::Result<
            tonic::Response<super::ExecuteQueryResult>,
            tonic::Status,
        >;
        async fn get_job_status(
            &self,
            request: tonic::Request<super::GetJobStatusParams>,
        ) -> std::result::Result<
            tonic::Response<super::GetJobStatusResult>,
            tonic::Status,
        >;
        /// Used by Executor to tell Scheduler it is stopped.
        async fn executor_stopped(
            &self,
            request: tonic::Request<super::ExecutorStoppedParams>,
        ) -> std::result::Result<
            tonic::Response<super::ExecutorStoppedResult>,
            tonic::Status,
        >;
        async fn cancel_job(
            &self,
            request: tonic::Request<super::CancelJobParams>,
        ) -> std::result::Result<tonic::Response<super::CancelJobResult>, tonic::Status>;
        async fn clean_job_data(
            &self,
            request: tonic::Request<super::CleanJobDataParams>,
        ) -> std::result::Result<
            tonic::Response<super::CleanJobDataResult>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct SchedulerGrpcServer<T: SchedulerGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: SchedulerGrpc> SchedulerGrpcServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for SchedulerGrpcServer<T>
    where
        T: SchedulerGrpc,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/ballista.protobuf.SchedulerGrpc/PollWork" => {
                    #[allow(non_camel_case_types)]
                    struct PollWorkSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::PollWorkParams>
                    for PollWorkSvc<T> {
                        type Response = super::PollWorkResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PollWorkParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::poll_work(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PollWorkSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/RegisterExecutor" => {
                    #[allow(non_camel_case_types)]
                    struct RegisterExecutorSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::RegisterExecutorParams>
                    for RegisterExecutorSvc<T> {
                        type Response = super::RegisterExecutorResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RegisterExecutorParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::register_executor(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RegisterExecutorSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/HeartBeatFromExecutor" => {
                    #[allow(non_camel_case_types)]
                    struct HeartBeatFromExecutorSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::HeartBeatParams>
                    for HeartBeatFromExecutorSvc<T> {
                        type Response = super::HeartBeatResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::HeartBeatParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::heart_beat_from_executor(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = HeartBeatFromExecutorSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/UpdateTaskStatus" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateTaskStatusSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::UpdateTaskStatusParams>
                    for UpdateTaskStatusSvc<T> {
                        type Response = super::UpdateTaskStatusResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateTaskStatusParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::update_task_status(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateTaskStatusSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/GetFileMetadata" => {
                    #[allow(non_camel_case_types)]
                    struct GetFileMetadataSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::GetFileMetadataParams>
                    for GetFileMetadataSvc<T> {
                        type Response = super::GetFileMetadataResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetFileMetadataParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::get_file_metadata(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetFileMetadataSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/CreateSession" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSessionSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::CreateSessionParams>
                    for CreateSessionSvc<T> {
                        type Response = super::CreateSessionResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateSessionParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::create_session(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateSessionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/UpdateSession" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateSessionSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::UpdateSessionParams>
                    for UpdateSessionSvc<T> {
                        type Response = super::UpdateSessionResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateSessionParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::update_session(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateSessionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/RemoveSession" => {
                    #[allow(non_camel_case_types)]
                    struct RemoveSessionSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::RemoveSessionParams>
                    for RemoveSessionSvc<T> {
                        type Response = super::RemoveSessionResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RemoveSessionParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::remove_session(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RemoveSessionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/ExecuteQuery" => {
                    #[allow(non_camel_case_types)]
                    struct ExecuteQuerySvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::ExecuteQueryParams>
                    for ExecuteQuerySvc<T> {
                        type Response = super::ExecuteQueryResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ExecuteQueryParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::execute_query(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ExecuteQuerySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/GetJobStatus" => {
                    #[allow(non_camel_case_types)]
                    struct GetJobStatusSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::GetJobStatusParams>
                    for GetJobStatusSvc<T> {
                        type Response = super::GetJobStatusResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetJobStatusParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::get_job_status(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetJobStatusSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/ExecutorStopped" => {
                    #[allow(non_camel_case_types)]
                    struct ExecutorStoppedSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::ExecutorStoppedParams>
                    for ExecutorStoppedSvc<T> {
                        type Response = super::ExecutorStoppedResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ExecutorStoppedParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::executor_stopped(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ExecutorStoppedSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/CancelJob" => {
                    #[allow(non_camel_case_types)]
                    struct CancelJobSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::CancelJobParams>
                    for CancelJobSvc<T> {
                        type Response = super::CancelJobResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CancelJobParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::cancel_job(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CancelJobSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.SchedulerGrpc/CleanJobData" => {
                    #[allow(non_camel_case_types)]
                    struct CleanJobDataSvc<T: SchedulerGrpc>(pub Arc<T>);
                    impl<
                        T: SchedulerGrpc,
                    > tonic::server::UnaryService<super::CleanJobDataParams>
                    for CleanJobDataSvc<T> {
                        type Response = super::CleanJobDataResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CleanJobDataParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as SchedulerGrpc>::clean_job_data(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CleanJobDataSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: SchedulerGrpc> Clone for SchedulerGrpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: SchedulerGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: SchedulerGrpc> tonic::server::NamedService for SchedulerGrpcServer<T> {
        const NAME: &'static str = "ballista.protobuf.SchedulerGrpc";
    }
}
/// Generated server implementations.
pub mod executor_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ExecutorGrpcServer.
    #[async_trait]
    pub trait ExecutorGrpc: Send + Sync + 'static {
        async fn launch_task(
            &self,
            request: tonic::Request<super::LaunchTaskParams>,
        ) -> std::result::Result<
            tonic::Response<super::LaunchTaskResult>,
            tonic::Status,
        >;
        async fn launch_multi_task(
            &self,
            request: tonic::Request<super::LaunchMultiTaskParams>,
        ) -> std::result::Result<
            tonic::Response<super::LaunchMultiTaskResult>,
            tonic::Status,
        >;
        async fn stop_executor(
            &self,
            request: tonic::Request<super::StopExecutorParams>,
        ) -> std::result::Result<
            tonic::Response<super::StopExecutorResult>,
            tonic::Status,
        >;
        async fn cancel_tasks(
            &self,
            request: tonic::Request<super::CancelTasksParams>,
        ) -> std::result::Result<
            tonic::Response<super::CancelTasksResult>,
            tonic::Status,
        >;
        async fn remove_job_data(
            &self,
            request: tonic::Request<super::RemoveJobDataParams>,
        ) -> std::result::Result<
            tonic::Response<super::RemoveJobDataResult>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ExecutorGrpcServer<T: ExecutorGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ExecutorGrpc> ExecutorGrpcServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ExecutorGrpcServer<T>
    where
        T: ExecutorGrpc,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/ballista.protobuf.ExecutorGrpc/LaunchTask" => {
                    #[allow(non_camel_case_types)]
                    struct LaunchTaskSvc<T: ExecutorGrpc>(pub Arc<T>);
                    impl<
                        T: ExecutorGrpc,
                    > tonic::server::UnaryService<super::LaunchTaskParams>
                    for LaunchTaskSvc<T> {
                        type Response = super::LaunchTaskResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LaunchTaskParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ExecutorGrpc>::launch_task(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = LaunchTaskSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.ExecutorGrpc/LaunchMultiTask" => {
                    #[allow(non_camel_case_types)]
                    struct LaunchMultiTaskSvc<T: ExecutorGrpc>(pub Arc<T>);
                    impl<
                        T: ExecutorGrpc,
                    > tonic::server::UnaryService<super::LaunchMultiTaskParams>
                    for LaunchMultiTaskSvc<T> {
                        type Response = super::LaunchMultiTaskResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LaunchMultiTaskParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ExecutorGrpc>::launch_multi_task(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = LaunchMultiTaskSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.ExecutorGrpc/StopExecutor" => {
                    #[allow(non_camel_case_types)]
                    struct StopExecutorSvc<T: ExecutorGrpc>(pub Arc<T>);
                    impl<
                        T: ExecutorGrpc,
                    > tonic::server::UnaryService<super::StopExecutorParams>
                    for StopExecutorSvc<T> {
                        type Response = super::StopExecutorResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StopExecutorParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ExecutorGrpc>::stop_executor(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = StopExecutorSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.ExecutorGrpc/CancelTasks" => {
                    #[allow(non_camel_case_types)]
                    struct CancelTasksSvc<T: ExecutorGrpc>(pub Arc<T>);
                    impl<
                        T: ExecutorGrpc,
                    > tonic::server::UnaryService<super::CancelTasksParams>
                    for CancelTasksSvc<T> {
                        type Response = super::CancelTasksResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CancelTasksParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ExecutorGrpc>::cancel_tasks(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CancelTasksSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/ballista.protobuf.ExecutorGrpc/RemoveJobData" => {
                    #[allow(non_camel_case_types)]
                    struct RemoveJobDataSvc<T: ExecutorGrpc>(pub Arc<T>);
                    impl<
                        T: ExecutorGrpc,
                    > tonic::server::UnaryService<super::RemoveJobDataParams>
                    for RemoveJobDataSvc<T> {
                        type Response = super::RemoveJobDataResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RemoveJobDataParams>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ExecutorGrpc>::remove_job_data(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RemoveJobDataSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: ExecutorGrpc> Clone for ExecutorGrpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: ExecutorGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ExecutorGrpc> tonic::server::NamedService for ExecutorGrpcServer<T> {
        const NAME: &'static str = "ballista.protobuf.ExecutorGrpc";
    }
}
