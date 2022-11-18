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

use ballista::prelude::{BallistaConfig, BallistaContext, Result};
use datafusion::prelude::CsvReadOptions;

#[tokio::main]
async fn main() -> Result<()> {
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "1")
        .build()?;

    let ctx = BallistaContext::standalone(&config, 2, None).await?;

    // register csv file with the execution context
    ctx.register_csv(
        "test",
        "testdata/aggregate_test_100.csv",
        CsvReadOptions::new(),
    )
    .await?;

    let df = ctx.sql("select count(1) from test").await?;

    df.show().await?;
    Ok(())
}
