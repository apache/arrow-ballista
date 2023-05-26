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

#[cfg(unix)]
use tokio::signal::unix::SignalKind;
#[cfg(unix)]
use tokio::signal::unix::{self as os_impl};
#[cfg(windows)]
use tokio::signal::windows::{self as os_impl};

use std::io;

pub async fn sig_term() -> io::Result<()> {
    #[cfg(unix)]
    os_impl::signal(SignalKind::terminate())?.recv().await;
    #[cfg(windows)]
    // TODO fix windows terminate after upgrading to latest tokio
    os_impl::ctrl_break()?.recv().await;
    Ok(())
}
