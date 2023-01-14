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
set -x

# This bash script is meant to be run inside the docker-compose environment. Check the README for instructions

# regression checks for queries that return the correct results
# TODO add all queries once https://github.com/apache/arrow-datafusion/issues/3478 is implemented and once
# queries return decimal results with the correct precision
for query in 4 12 13
do
  /root/tpch benchmark ballista --host ballista-scheduler --port 50050 --query $query --path /data --format tbl --iterations 1 --debug --expected /data
done

# at least make sure these queries run, even though we do not check that the results are correct yet

#TODO: add query 16 once we support it
for query in 1 2 3 5 6 7 8 9 10 11 14 15 17 18 19 20 21 22
do
  python3 tpch.py --host ballista-scheduler --query $query --path /data --ext tbl 
done

