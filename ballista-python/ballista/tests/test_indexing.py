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

import pyarrow as pa
import pytest

from datafusion import SessionContext


@pytest.fixture
def df():
    ctx = SessionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 4, 6])],
        names=["a", "b"],
    )
    return ctx.create_dataframe([[batch]])


def test_indexing(df):
    assert df["a"] is not None
    assert df["a", "b"] is not None
    assert df[("a", "b")] is not None
    assert df[["a"]] is not None


def test_err(df):
    with pytest.raises(Exception) as e_info:
        df["c"]

    assert "Schema error: No field named 'c'" in e_info.value.args[0]

    with pytest.raises(Exception) as e_info:
        df[1]

    assert (
        "DataFrame can only be indexed by string index or indices"
        in e_info.value.args[0]
    )
