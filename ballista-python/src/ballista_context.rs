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

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::path::PathBuf;

use crate::errors::BallistaError;
use crate::utils::wait_for_future;

use crate::dataframe::PyDataFrame;
use ballista::prelude::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::{AvroReadOptions, CsvReadOptions, ParquetReadOptions};

/// `PyBallistaContext` is able to plan and execute DataFusion plans.
/// It has a powerful optimizer, a physical planner for local execution, and a
/// multi-threaded execution engine to perform the execution.
#[pyclass(name = "BallistaContext", module = "ballista", subclass, unsendable)]
pub(crate) struct PyBallistaContext {
    ctx: BallistaContext,
}

#[pymethods]
impl PyBallistaContext {
    #[new]
    #[args(port = "50050")]
    fn new(py: Python, host: &str, port: u16) -> PyResult<Self> {
        let config = BallistaConfig::builder()
            .set("ballista.shuffle.partitions", "4")
            .build()
            .map_err(BallistaError::from)?;

        let result = BallistaContext::remote(host, port, &config);
        let ctx = wait_for_future(py, result).map_err(BallistaError::from)?;

        Ok(PyBallistaContext { ctx })
    }

    /// Returns a PyDataFrame whose plan corresponds to the SQL statement.
    fn sql(&mut self, query: &str, py: Python) -> PyResult<PyDataFrame> {
        let ctx = &self.ctx;

        let result = ctx.sql(query);
        let df = wait_for_future(py, result).map_err(BallistaError::from)?;
        Ok(PyDataFrame::new(df))
    }

    #[allow(clippy::too_many_arguments)]
    #[args(
        schema = "None",
        has_header = "true",
        delimiter = "\",\"",
        schema_infer_max_records = "1000",
        file_extension = "\".csv\""
    )]
    fn register_csv(
        &mut self,
        name: &str,
        path: PathBuf,
        schema: Option<Schema>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        py: Python,
    ) -> PyResult<()> {
        let ctx = &self.ctx;

        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;

        let delimiter = delimiter.as_bytes();
        if delimiter.len() != 1 {
            return Err(PyValueError::new_err(
                "Delimiter must be a single character",
            ));
        }

        let mut options = CsvReadOptions::new()
            .has_header(has_header)
            .delimiter(delimiter[0])
            .schema_infer_max_records(schema_infer_max_records)
            .file_extension(file_extension);
        options.schema = schema.as_ref();

        let result = ctx.register_csv(name, path, options);
        wait_for_future(py, result).map_err(BallistaError::from)?;

        Ok(())
    }

    fn register_avro(&mut self, name: &str, path: &str, py: Python) -> PyResult<()> {
        let ctx = &self.ctx;

        let result = ctx.register_avro(name, path, AvroReadOptions::default());
        wait_for_future(py, result).map_err(BallistaError::from)?;

        Ok(())
    }

    fn register_parquet(&mut self, name: &str, path: &str, py: Python) -> PyResult<()> {
        let ctx = &self.ctx;

        let result = ctx.register_parquet(name, path, ParquetReadOptions::default());
        wait_for_future(py, result).map_err(BallistaError::from)?;

        Ok(())
    }
}
