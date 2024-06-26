<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Ballista: Distributed Scheduler for Apache DataFusion

Ballista is a distributed compute platform primarily implemented in Rust, and powered by Apache Arrow and
DataFusion. It is built on an architecture that allows other programming languages (such as Python, C++, and
Java) to be supported as first-class citizens without paying a penalty for serialization costs.

The foundational technologies in Ballista are:

- [Apache Arrow](https://arrow.apache.org/) memory model and compute kernels for efficient processing of data.
- [Apache Arrow Flight Protocol](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) for efficient
  data transfer between processes.
- [Google Protocol Buffers](https://developers.google.com/protocol-buffers) for serializing query plans.
- [Docker](https://www.docker.com/) for packaging up executors along with user-defined code.

Ballista can be deployed as a standalone cluster and also supports [Kubernetes](https://kubernetes.io/). In either
case, the scheduler can be configured to use [etcd](https://etcd.io/) as a backing store to (eventually) provide
redundancy in the case of a scheduler failing.

## Rust Version Compatibility

This crate is tested with the latest stable version of Rust. We do not currrently test against other, older versions of the Rust compiler.

## Starting a cluster

There are numerous ways to start a Ballista cluster, including support for Docker and
Kubernetes. For full documentation, refer to the deployment section of the
[Ballista User Guide](https://datafusion.apache.org/ballista/user-guide/deployment/)

A simple way to start a local cluster for testing purposes is to use cargo to install
the scheduler and executor crates.

```bash
cargo install --locked ballista-scheduler
cargo install --locked ballista-executor
```

With these crates installed, it is now possible to start a scheduler process.

```bash
RUST_LOG=info ballista-scheduler
```

The scheduler will bind to port `50050` by default.

Next, start an executor processes in a new terminal session with the specified concurrency
level.

```bash
RUST_LOG=info ballista-executor -c 4
```

The executor will bind to port `50051` by default. Additional executors can be started by
manually specifying a bind port. For example:

```bash
RUST_LOG=info ballista-executor --bind-port 50052 -c 4
```

## Executing a query

Ballista provides a `BallistaContext` as a starting point for creating queries. DataFrames can be created
by invoking the `read_csv`, `read_parquet`, and `sql` methods.

To build a simple ballista example, run the following command to add the dependencies to your `Cargo.toml` file:

```bash
cargo add ballista datafusion tokio
```

The following example runs a simple aggregate SQL query against a Parquet file (`yellow_tripdata_2022-01.parquet`) from the
[New York Taxi and Limousine Commission](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
data set. Download the file and add it to the `testdata` folder before running the example.

```rust,no_run
use ballista::prelude::*;
use datafusion::prelude::{col, min, max, avg, sum, ParquetReadOptions};

#[tokio::main]
async fn main() -> Result<()> {
    // create configuration
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "4")
        .build()?;

    // connect to Ballista scheduler
    let ctx = BallistaContext::remote("localhost", 50050, &config).await?;

    let filename = "testdata/yellow_tripdata_2022-01.parquet";

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?
        .select_columns(&["passenger_count", "fare_amount"])?
        .aggregate(
            vec![col("passenger_count")],
            vec![
                min(col("fare_amount")),
                max(col("fare_amount")),
                avg(col("fare_amount")),
                sum(col("fare_amount")),
            ],
        )?
        .sort(vec![col("passenger_count").sort(true, true)])?;

    // this is equivalent to the following SQL
    // SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), AVG(fare_amount), SUM(fare_amount)
    //     FROM tripdata
    //     GROUP BY passenger_count
    //     ORDER BY passenger_count

    // print the results
    df.show().await?;

    Ok(())
}
```

The output should look similar to the following table.

```{r eval=FALSE}
+-----------------+--------------------------+--------------------------+--------------------------+--------------------------+
| passenger_count | MIN(?table?.fare_amount) | MAX(?table?.fare_amount) | AVG(?table?.fare_amount) | SUM(?table?.fare_amount) |
+-----------------+--------------------------+--------------------------+--------------------------+--------------------------+
|                 | -159.5                   | 285.2                    | 17.60577640099004        | 1258865.829999991        |
| 0               | -115                     | 500                      | 11.794859107585335       | 614052.1600000001        |
| 1               | -480                     | 401092.32                | 12.61028389876563        | 22623542.879999973       |
| 2               | -250                     | 640.5                    | 13.79501011585127        | 4732047.139999998        |
| 3               | -130                     | 480                      | 13.473184817311106       | 1139427.2400000002       |
| 4               | -250                     | 464                      | 14.232650547832726       | 502711.4499999997        |
| 5               | -52                      | 668                      | 12.160378472086954       | 624289.51                |
| 6               | -52                      | 252.5                    | 12.576583325529857       | 402916                   |
| 7               | 7                        | 79                       | 61.77777777777778        | 556                      |
| 8               | 8.3                      | 115                      | 79.9125                  | 639.3                    |
| 9               | 9.3                      | 96.5                     | 65.26666666666667        | 195.8                    |
+-----------------+--------------------------+--------------------------+--------------------------+--------------------------+
```

More [examples](https://github.com/apache/arrow-ballista/tree/main/examples) can be found in the arrow-ballista repository.
