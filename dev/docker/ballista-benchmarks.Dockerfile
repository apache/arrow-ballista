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

FROM ubuntu:22.04

ARG RELEASE_FLAG=release

ENV RELEASE_FLAG=${RELEASE_FLAG}
ENV RUST_LOG=info
ENV RUST_BACKTRACE=full
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get -y install python3 python3-pip

COPY target/$RELEASE_FLAG/ballista-scheduler /root/ballista-scheduler
COPY target/$RELEASE_FLAG/ballista-executor /root/ballista-executor
COPY target/$RELEASE_FLAG/tpch /root/tpch
COPY python/target/wheels/ballista-*-manylinux*.whl /root/
RUN pip3 install /root/ballista-*-manylinux*.whl

COPY benchmarks/run.sh /root/run.sh
COPY benchmarks/tpch.py /root/tpch.py
COPY benchmarks/queries/ /root/benchmarks/queries

WORKDIR /root

CMD ["/root/run.sh"]