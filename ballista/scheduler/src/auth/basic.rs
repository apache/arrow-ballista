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

use crate::auth::Authorizer;
use async_trait::async_trait;

use tonic::Status;

pub struct BasicAuthorizer {
    username: String,
    password: String,
}

impl BasicAuthorizer {
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
}

#[async_trait]
impl Authorizer for BasicAuthorizer {
    async fn validate(&self, value: &str) -> Result<(), Status> {
        let basic = "Basic ";
        if !value.starts_with(basic) {
            Err(Status::invalid_argument(format!(
                "Auth type not implemented: {value}"
            )))?;
        }
        let base64 = &value[basic.len()..];
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
        if user != self.username || pass != self.password {
            Err(Status::unauthenticated("Invalid credentials!"))?
        }
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn validate_valid_creds() {
        let a = BasicAuthorizer {
            username: "foo".to_string(),
            password: "bar".to_string(),
        };
        a.validate("Basic Zm9vOmJhcg==")
            .await
            .expect("auth should succeed")
    }

    #[tokio::test]
    async fn validate_invalid_username() {
        let a = BasicAuthorizer {
            username: "foo".to_string(),
            password: "bar".to_string(),
        };
        let e = a
            .validate("Basic YmF6OmJhcg==")
            .await
            .expect_err("auth with invalid username should fail");
        assert_eq!(
            e.to_string(),
            Status::unauthenticated("Invalid credentials!").to_string()
        );
    }

    #[tokio::test]
    async fn validate_invalid_password() {
        let a = BasicAuthorizer {
            username: "foo".to_string(),
            password: "bar".to_string(),
        };
        let e = a
            .validate("Basic Zm9vOmJheg==")
            .await
            .expect_err("auth should fail with invalid password");
        assert_eq!(
            e.to_string(),
            Status::unauthenticated("Invalid credentials!").to_string()
        );
    }

    #[tokio::test]
    async fn validate_invalid_not_split_by_colon() {
        let a = BasicAuthorizer {
            username: "foo".to_string(),
            password: "bar".to_string(),
        };
        let e = a
            .validate("Basic Zm9vYmF6")
            .await
            .expect_err("auth should fail if not properly formatted as basic auth");
        assert_eq!(
            e.to_string(),
            Status::invalid_argument("Invalid authorization header").to_string()
        );
    }

    #[tokio::test]
    async fn validate_invalid_not_base64_encoded() {
        let a = BasicAuthorizer {
            username: "foo".to_string(),
            password: "bar".to_string(),
        };
        let e = a
            .validate("Basic foo-baz")
            .await
            .expect_err("auth should fail with invalid password");
        assert_eq!(
            e.to_string(),
            Status::invalid_argument("authorization not parsable").to_string()
        );
    }
}
