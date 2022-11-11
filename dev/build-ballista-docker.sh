#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

RELEASE_FLAG=${RELEASE_FLAG:=release}

docker build -t ballista-builder --build-arg EXT_UID="$(id -u)" -f dev/docker/ballista-builder.Dockerfile .

docker run -v $(pwd):/home/builder/workspace --env RELEASE_FLAG=$RELEASE_FLAG ballista-builder

docker-compose build

. ./dev/build-set-env.sh
docker build -t "apache/arrow-ballista-benchmarks:$BALLISTA_VERSION" -f dev/docker/ballista-standalone.Dockerfile .
docker tag ballista-executor "apache/arrow-ballista-executor:$BALLISTA_VERSION"
docker tag ballista-scheduler "apache/arrow-ballista-scheduler:$BALLISTA_VERSION"
docker tag ballista-benchmarks "apache/arrow-ballista-benchmarks:$BALLISTA_VERSION"
