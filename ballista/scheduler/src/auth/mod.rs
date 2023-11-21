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

pub mod basic;
pub mod jwt;

use std::sync::Arc;

use crate::auth::basic::BasicAuthorizer;
use crate::auth::jwt::JWTAuthorizer;
use crate::config::SchedulerConfig;
use async_trait::async_trait;

#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn validate(&self, value: &str) -> Result<(), tonic::Status>;
}

pub fn default_handshake_authorizer() -> Arc<impl Authorizer> {
    return Arc::new(BasicAuthorizer::new(
        "admin".to_string(),
        "password".to_string(),
    ));
}

pub fn authorizer_from_config(
    config: Arc<SchedulerConfig>,
) -> ballista_core::error::Result<Arc<dyn Authorizer>> {
    let auth_method = config.client_auth_method.clone();
    match auth_method.as_str() {
        "basic" => Ok(Arc::new(BasicAuthorizer::new(
            config.client_auth_basic_username.clone(),
            config.client_auth_basic_password.clone(),
        ))),
        "jwt" => Ok(Arc::new(JWTAuthorizer {})),
        _ => Err(ballista_core::error::BallistaError::NotImplemented(
            format!("{} client auth method is not implemented", auth_method),
        )),
    }
}
