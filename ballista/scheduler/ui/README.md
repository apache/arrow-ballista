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

# Ballista UI

## Start project from source

### Pre-requisites

To view the job graph, you must first install [Graphviz](https://graphviz.org/download/)
on your machine.

### Run scheduler/executor

First, run scheduler from project:

```shell
$ cd ballista/scheduler
$ RUST_LOG=info cargo run --release
...
    Finished release [optimized] target(s) in 11.92s
    Running `/path-to-project/target/release/ballista-scheduler`
```

and run executor in new terminal:

```shell
$ cd ballista/executor
$ RUST_LOG=info cargo run --release
    Finished release [optimized] target(s) in 0.09s
    Running `/path-to-project/target/release/ballista-executor`
```

### Run Client project

```shell
$ cd ballista/scheduler/ui
$ yarn
Resolving packages...
$ yarn start
Starting the development server...
```

Now access to http://localhost:3000/

**NOTE**: If you get an error when running the `yarn start` command above, make sure
to use the `lts` version of npm.

You can install it by running: `npm install --lts`. This should fix any errors that
may occur when trying to start the Ballista UI project.
