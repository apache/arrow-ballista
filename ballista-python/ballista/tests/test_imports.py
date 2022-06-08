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

import pytest

import ballista
from ballista import (
    AggregateUDF,
    DataFrame,
    SessionContext,
    Expression,
    ScalarUDF,
    functions,
)


def test_import_ballista():
    assert ballista.__name__ == "ballista"


def test_ballista_python_version():
    assert ballista.__version__ is not None


def test_class_module_is_ballista():
    for klass in [
        SessionContext,
        Expression,
        DataFrame,
        ScalarUDF,
        AggregateUDF,
    ]:
        assert klass.__module__ == "ballista"


def test_import_from_functions_submodule():
    from ballista.functions import abs, sin  # noqa

    assert functions.abs is abs
    assert functions.sin is sin

    msg = "cannot import name 'foobar' from 'ballista.functions'"
    with pytest.raises(ImportError, match=msg):
        from ballista.functions import foobar  # noqa


def test_classes_are_inheritable():
    class MyExecContext(SessionContext):
        pass

    class MyExpression(Expression):
        pass

    class MyDataFrame(DataFrame):
        pass
