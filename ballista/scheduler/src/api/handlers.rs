// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::SchedulerServer;
use crate::state::execution_graph::ExecutionStage;
use crate::state::execution_graph_dot::ExecutionGraphDot;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::BALLISTA_VERSION;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet, Time};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use graphviz_rust::cmd::{CommandArg, Format};
use graphviz_rust::exec;
use graphviz_rust::printer::PrinterContext;
use http::header::CONTENT_TYPE;

use std::time::Duration;
use warp::Rejection;

#[derive(Debug, serde::Serialize)]
struct SchedulerStateResponse {
    started: u128,
    version: &'static str,
}

#[allow(dead_code)]
#[derive(Debug, serde::Serialize)]
struct ExecutorsResponse {
    executors: Vec<ExecutorMetaResponse>,
}

#[derive(Debug, serde::Serialize)]
pub struct ExecutorMetaResponse {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub last_seen: u128,
}

#[derive(Debug, serde::Serialize)]
pub struct JobResponse {
    pub job_id: String,
    pub job_name: String,
    pub job_status: String,
    pub num_stages: usize,
    pub completed_stages: usize,
    pub percent_complete: u8,
}

#[derive(Debug, serde::Serialize)]
struct CancelJobResponse {
    pub cancelled: bool,
}

#[derive(Debug, serde::Serialize)]
pub struct QueryStageSummary {
    pub stage_id: String,
    pub stage_status: String,
    pub input_rows: usize,
    pub output_rows: usize,
    pub elapsed_compute: String,
}

/// Return current scheduler state
pub(crate) async fn get_scheduler_state<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
) -> Result<impl warp::Reply, Rejection> {
    let response = SchedulerStateResponse {
        started: data_server.start_time,
        version: BALLISTA_VERSION,
    };
    Ok(warp::reply::json(&response))
}

/// Return list of executors
pub(crate) async fn get_executors<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
) -> Result<impl warp::Reply, Rejection> {
    let state = data_server.state;
    let executors: Vec<ExecutorMetaResponse> = state
        .executor_manager
        .get_executor_state()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(metadata, duration)| ExecutorMetaResponse {
            id: metadata.id,
            host: metadata.host,
            port: metadata.port,
            last_seen: duration.as_millis(),
        })
        .collect();

    Ok(warp::reply::json(&executors))
}

/// Return list of jobs
pub(crate) async fn get_jobs<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
) -> Result<impl warp::Reply, Rejection> {
    // TODO: Display last seen information in UI
    let state = data_server.state;

    let jobs = state
        .task_manager
        .get_jobs()
        .await
        .map_err(|_| warp::reject())?;

    let jobs: Vec<JobResponse> = jobs
        .iter()
        .map(|job| {
            let status = &job.status;
            let job_status = match &status.status {
                Some(Status::Queued(_)) => "Queued".to_string(),
                Some(Status::Running(_)) => "Running".to_string(),
                Some(Status::Failed(error)) => format!("Failed: {}", error.error),
                Some(Status::Successful(completed)) => {
                    let num_rows = completed
                        .partition_location
                        .iter()
                        .map(|p| {
                            p.partition_stats.as_ref().map(|s| s.num_rows).unwrap_or(0)
                        })
                        .sum::<i64>();
                    let num_rows_term = if num_rows == 1 { "row" } else { "rows" };
                    let num_partitions = completed.partition_location.len();
                    let num_partitions_term = if num_partitions == 1 {
                        "partition"
                    } else {
                        "partitions"
                    };
                    format!(
                        "Completed. Produced {} {} containing {} {}. Elapsed time: {} ms.",
                        num_partitions, num_partitions_term, num_rows, num_rows_term,
                        job.end_time - job.start_time
                    )
                }
                _ => "Invalid State".to_string(),
            };

            // calculate progress based on completed stages for now, but we could use completed
            // tasks in the future to make this more accurate
            let percent_complete =
                ((job.completed_stages as f32 / job.num_stages as f32) * 100_f32) as u8;
            JobResponse {
                job_id: job.job_id.to_string(),
                job_name: job.job_name.to_string(),
                job_status,
                num_stages: job.num_stages,
                completed_stages: job.completed_stages,
                percent_complete,
            }
        })
        .collect();

    Ok(warp::reply::json(&jobs))
}

pub(crate) async fn cancel_job<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
    job_id: String,
) -> Result<impl warp::Reply, Rejection> {
    // 404 if job doesn't exist
    data_server
        .state
        .task_manager
        .get_job_status(&job_id)
        .await
        .map_err(|_| warp::reject())?
        .ok_or_else(warp::reject)?;

    data_server
        .query_stage_event_loop
        .get_sender()
        .map_err(|_| warp::reject())?
        .post_event(QueryStageSchedulerEvent::JobCancel(job_id))
        .await
        .map_err(|_| warp::reject())?;

    Ok(warp::reply::json(&CancelJobResponse { cancelled: true }))
}

#[derive(Debug, serde::Serialize)]
pub struct QueryStagesResponse {
    pub stages: Vec<QueryStageSummary>,
}

/// Get the execution graph for the specified job id
pub(crate) async fn get_query_stages<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
    job_id: String,
) -> Result<impl warp::Reply, Rejection> {
    if let Some(graph) = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id)
        .await
        .map_err(|_| warp::reject())?
    {
        Ok(warp::reply::json(&QueryStagesResponse {
            stages: graph
                .as_ref()
                .stages()
                .iter()
                .map(|(id, stage)| {
                    let mut summary = QueryStageSummary {
                        stage_id: id.to_string(),
                        stage_status: stage.variant_name().to_string(),
                        input_rows: 0,
                        output_rows: 0,
                        elapsed_compute: "".to_string(),
                    };
                    match stage {
                        ExecutionStage::Running(running_stage) => {
                            summary.input_rows = running_stage
                                .stage_metrics
                                .as_ref()
                                .map(|m| get_combined_count(m.as_slice(), "input_rows"))
                                .unwrap_or(0);
                            summary.output_rows = running_stage
                                .stage_metrics
                                .as_ref()
                                .map(|m| get_combined_count(m.as_slice(), "output_rows"))
                                .unwrap_or(0);
                            summary.elapsed_compute = running_stage
                                .stage_metrics
                                .as_ref()
                                .map(|m| get_elapsed_compute_nanos(m.as_slice()))
                                .unwrap_or_default();
                        }
                        ExecutionStage::Successful(completed_stage) => {
                            summary.input_rows = get_combined_count(
                                &completed_stage.stage_metrics,
                                "input_rows",
                            );
                            summary.output_rows = get_combined_count(
                                &completed_stage.stage_metrics,
                                "output_rows",
                            );
                            summary.elapsed_compute =
                                get_elapsed_compute_nanos(&completed_stage.stage_metrics);
                        }
                        _ => {}
                    }
                    summary
                })
                .collect(),
        }))
    } else {
        Ok(warp::reply::json(&QueryStagesResponse { stages: vec![] }))
    }
}

fn get_elapsed_compute_nanos(metrics: &[MetricsSet]) -> String {
    let nanos: usize = metrics
        .iter()
        .flat_map(|vec| {
            vec.iter().map(|metric| match metric.as_ref().value() {
                MetricValue::ElapsedCompute(time) => time.value(),
                _ => 0,
            })
        })
        .sum();
    let t = Time::new();
    t.add_duration(Duration::from_nanos(nanos as u64));
    t.to_string()
}

fn get_combined_count(metrics: &[MetricsSet], name: &str) -> usize {
    metrics
        .iter()
        .flat_map(|vec| {
            vec.iter().map(|metric| {
                let metric_value = metric.value();
                if metric_value.name() == name {
                    metric_value.as_usize()
                } else {
                    0
                }
            })
        })
        .sum()
}

/// Generate a dot graph for the specified job id and return as plain text
pub(crate) async fn get_job_dot_graph<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
    job_id: String,
) -> Result<String, Rejection> {
    if let Some(graph) = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id)
        .await
        .map_err(|_| warp::reject())?
    {
        ExecutionGraphDot::generate(graph.as_ref()).map_err(|_| warp::reject())
    } else {
        Ok("Not Found".to_string())
    }
}

/// Generate a dot graph for the specified job id and query stage and return as plain text
pub(crate) async fn get_query_stage_dot_graph<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
    job_id: String,
    stage_id: usize,
) -> Result<String, Rejection> {
    if let Some(graph) = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id)
        .await
        .map_err(|_| warp::reject())?
    {
        ExecutionGraphDot::generate_for_query_stage(graph.as_ref(), stage_id)
            .map_err(|_| warp::reject())
    } else {
        Ok("Not Found".to_string())
    }
}

/// Generate an SVG graph for the specified job id and return it as plain text
pub(crate) async fn get_job_svg_graph<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
    job_id: String,
) -> Result<String, Rejection> {
    let dot = get_job_dot_graph(data_server, job_id).await;
    match dot {
        Ok(dot) => {
            let graph = graphviz_rust::parse(&dot);
            if let Ok(graph) = graph {
                exec(
                    graph,
                    &mut PrinterContext::default(),
                    vec![CommandArg::Format(Format::Svg)],
                )
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                .map_err(|_| warp::reject())
            } else {
                Ok("Cannot parse graph".to_string())
            }
        }
        _ => Ok("Not Found".to_string()),
    }
}

pub(crate) async fn get_scheduler_metrics<T: AsLogicalPlan, U: AsExecutionPlan>(
    data_server: SchedulerServer<T, U>,
) -> Result<impl warp::Reply, Rejection> {
    Ok(data_server
        .metrics_collector()
        .gather_metrics()
        .map_err(|_| warp::reject())?
        .map(|(data, content_type)| {
            warp::reply::with_header(data, CONTENT_TYPE, content_type)
        })
        .unwrap_or_else(|| warp::reply::with_header(vec![], CONTENT_TYPE, "text/html")))
}
