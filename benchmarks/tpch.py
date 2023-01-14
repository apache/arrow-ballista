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

import sys
import time
import argparse
import pyarrow as pa

parser = argparse.ArgumentParser(description='Run SQL benchmarks.')
parser.add_argument('--host', help='Ballista host')
parser.add_argument('--query', help='query to run, such as q1')
parser.add_argument('--path', help='path to data files')
parser.add_argument('--ext', default='', help='optional file extension, such as parquet')

args = parser.parse_args()

host = args.host
query = args.query
path = args.path
table_ext = args.ext


import ballista
ctx = ballista.BallistaContext(host, 50050)

tables = ["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]

table_schema = { "part": pa.schema([
    ("p_partkey", pa.int64()),
    ("p_name", pa.utf8()),
    ("p_mfgr", pa.utf8()),
    ("p_brand", pa.utf8()),
    ("p_type", pa.utf8()),
    ("p_size", pa.int32()),
    ("p_container", pa.utf8()),
    ("p_retailprice", pa.float64()),
    ("p_comment", pa.utf8()),
]),

"supplier": pa.schema([
    ("s_suppkey", pa.int64()),
    ("s_name", pa.utf8()),
    ("s_address", pa.utf8()),
    ("s_nationkey", pa.int64()),
    ("s_phone", pa.utf8()),
    ("s_acctbal", pa.float64()),
    ("s_comment", pa.utf8()),
]),

"partsupp": pa.schema([
    ("ps_partkey", pa.int64()),
    ("ps_suppkey", pa.int64()),
    ("ps_availqty", pa.int32()),
    ("ps_supplycost", pa.float64()),
    ("ps_comment", pa.utf8()),
]),

"customer": pa.schema([
    ("c_custkey", pa.int64()),
    ("c_name", pa.utf8()),
    ("c_address", pa.utf8()),
    ("c_nationkey", pa.int64()),
    ("c_phone", pa.utf8()),
    ("c_acctbal", pa.float64()),
    ("c_mktsegment", pa.utf8()),
    ("c_comment", pa.utf8()),
]),

"orders": pa.schema([
    ("o_orderkey", pa.int64()),
    ("o_custkey", pa.int64()),
    ("o_orderstatus", pa.utf8()),
    ("o_totalprice", pa.float64()),
    ("o_orderdate", pa.date32()),
    ("o_orderpriority", pa.utf8()),
    ("o_clerk", pa.utf8()),
    ("o_shippriority", pa.int32()),
    ("o_comment", pa.utf8()),
]),

"lineitem": pa.schema([
    ("l_orderkey", pa.int64()),
    ("l_partkey", pa.int64()),
    ("l_suppkey", pa.int64()),
    ("l_linenumber", pa.int32()),
    ("l_quantity", pa.float64()),
    ("l_extendedprice", pa.float64()),
    ("l_discount", pa.float64()),
    ("l_tax", pa.float64()),
    ("l_returnflag", pa.utf8()),
    ("l_linestatus", pa.utf8()),
    ("l_shipdate", pa.date32()),
    ("l_commitdate", pa.date32()),
    ("l_receiptdate", pa.date32()),
    ("l_shipinstruct", pa.utf8()),
    ("l_shipmode", pa.utf8()),
    ("l_comment", pa.utf8()),
]),

"nation": pa.schema([
    ("n_nationkey", pa.int64()),
    ("n_name", pa.utf8()),
    ("n_regionkey", pa.int64()),
    ("n_comment", pa.utf8()),
]),

"region": pa.schema([
    ("r_regionkey", pa.int64()),
    ("r_name", pa.utf8()),
    ("r_comment", pa.utf8()),
]) }


for table in tables:
    table_path = path + "/" + table
    if len(table_ext) > 0:
        table_path = table_path + "." + table_ext
    print("Registering table", table, "at path", table_path)
    ctx.register_csv(table, table_path, schema=table_schema[table], delimiter="|", has_header=False, file_extension="tbl")

with open("benchmarks/queries/q" + query + ".sql", 'r') as file:
    sql = file.read()

import time

start = time.time()

df = ctx.sql(sql)
results = df.collect()
row_count = sum([record.num_rows for record in results])

end = time.time()
print("Query", query, "took", end - start, "second(s) and returned", row_count,"rows")


