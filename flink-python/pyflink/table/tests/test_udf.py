################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction, udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class UserDefinedFunctionTests(PyFlinkStreamTableTestCase):

    def test_scalar_function(self):
        # test lambda function
        self.t_env.register_function(
            "add_one", udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT()))

        # test Python ScalarFunction
        self.t_env.register_function(
            "subtract_one", udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT()))

        # test Python function
        self.t_env.register_function("add", add)

        # test callable function
        self.t_env.register_function(
            "add_one_callable", udf(CallablePlus(), DataTypes.BIGINT(), DataTypes.BIGINT()))

        def partial_func(col, param):
            return col + param

        # test partial function
        import functools
        self.t_env.register_function(
            "add_one_partial",
            udf(functools.partial(partial_func, param=1), DataTypes.BIGINT(), DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd', 'e'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(),
             DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        t.where("add_one(b) <= 3") \
            .select("add_one(a), subtract_one(b), add(a, c), add_one_callable(a), "
                    "add_one_partial(a)") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2,1,4,2,2", "4,0,12,4,4"])

    def test_chaining_scalar_function(self):
        self.t_env.register_function(
            "add_one", udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT()))
        self.t_env.register_function(
            "subtract_one", udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT()))
        self.t_env.register_function("add", add)

        table_sink = source_sink_utils.TestAppendSink(['a'], [DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select("add(add_one(a), subtract_one(b))") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["3", "7", "4"])

    def test_udf_in_join_condition(self):
        t1 = self.t_env.from_elements([(2, "Hi")], ['a', 'b'])
        t2 = self.t_env.from_elements([(2, "Flink")], ['c', 'd'])

        self.t_env.register_function("f", udf(lambda i: i, DataTypes.BIGINT(), DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd'],
            [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING()])
        self.t_env.register_table_sink("Results", table_sink)

        t1.join(t2).where("f(a) = c").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2,Hi,2,Flink"])

    def test_udf_with_constant_param(self):
        def func_constant_param(p, null_param, tinyint_param, smallint_param, int_param,
                                bigint_param, decimal_param, float_param, double_param,
                                boolean_param, str_param,
                                date_param, time_param, timestamp_param):
            from decimal import Decimal
            import datetime
            if null_param is None:
                p += 1

            if isinstance(tinyint_param, int):
                p += tinyint_param
            if isinstance(smallint_param, int):
                p += smallint_param
            if isinstance(int_param, int):
                p += int_param
            if isinstance(bigint_param, int):
                p += bigint_param
            if isinstance(decimal_param, Decimal):
                p += int(decimal_param)
            if isinstance(float_param, float):
                p += int(float_param)
            if isinstance(double_param, float):
                p += float(double_param)

            if boolean_param is True:
                p += 1

            p += len(str_param)

            if date_param == datetime.date(year=2014, month=9, day=13):
                p += 1

            if time_param == datetime.time(hour=12, minute=0, second=0):
                p += 1

            if timestamp_param == datetime.datetime(1999, 9, 10, 5, 20, 10):
                p += 1

            return p

        self.t_env.register_function("func_constant_param",
                                     udf(func_constant_param,
                                         input_types=[DataTypes.BIGINT(),
                                                      DataTypes.BIGINT(),
                                                      DataTypes.TINYINT(),
                                                      DataTypes.SMALLINT(),
                                                      DataTypes.INT(),
                                                      DataTypes.BIGINT(),
                                                      DataTypes.DECIMAL(20, 10),
                                                      DataTypes.FLOAT(),
                                                      DataTypes.DOUBLE(),
                                                      DataTypes.BOOLEAN(),
                                                      DataTypes.STRING(),
                                                      DataTypes.DATE(),
                                                      DataTypes.TIME(),
                                                      DataTypes.TIMESTAMP()],
                                         result_type=DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(['a'], [DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        self.t_env.register_table("test_table", t)
        self.t_env.sql_query("select func_constant_param(a, "
                             "cast (null as BIGINT),"
                             "cast (1 as TINYINT),"
                             "cast (1 as SMALLINT),"
                             "cast (1 as INT),"
                             "cast (1 as BIGINT),"
                             "cast (1 as DECIMAL),"
                             "cast (1.0 as FLOAT),"
                             "cast (1.00 as DOUBLE),"
                             "true,"
                             "'flink',"
                             "cast ('2014-09-13' as DATE),"
                             "cast ('12:00:00' as TIME),"
                             "cast ('1999-9-10 05:20:10' as TIMESTAMP))"
                             " from test_table").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["18", "19", "20"])

    def test_udf_in_join_condition_2(self):
        t1 = self.t_env.from_elements([(1, "Hi"), (2, "Hi")], ['a', 'b'])
        t2 = self.t_env.from_elements([(2, "Flink")], ['c', 'd'])

        self.t_env.register_function("f", udf(lambda i: i, DataTypes.BIGINT(), DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd'],
            [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING()])
        self.t_env.register_table_sink("Results", table_sink)

        t1.join(t2).where("f(a) = f(c)").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2,Hi,2,Flink"])

    def test_overwrite_builtin_function(self):
        self.t_env.register_function(
            "plus", udf(lambda i, j: i + j - 1,
                        [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(['a'], [DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2, 3), (2, 5, 6), (3, 1, 9)], ['a', 'b', 'c'])
        t.select("plus(a, b)").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2", "6", "3"])

    def test_open(self):
        self.t_env.register_function(
            "subtract", udf(Subtract(), DataTypes.BIGINT(), DataTypes.BIGINT()))
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 4)], ['a', 'b'])
        t.select("a, subtract(b)").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,1", "2,4", "3,3"])

    def test_deterministic(self):
        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertTrue(add_one._deterministic)

        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT(), deterministic=False)
        self.assertFalse(add_one._deterministic)

        subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertTrue(subtract_one._deterministic)

        with self.assertRaises(ValueError, msg="Inconsistent deterministic: False and True"):
            udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT(), deterministic=False)

        self.assertTrue(add._deterministic)

        @udf(input_types=DataTypes.BIGINT(), result_type=DataTypes.BIGINT(), deterministic=False)
        def non_deterministic_udf(i):
            return i

        self.assertFalse(non_deterministic_udf._deterministic)

    def test_name(self):
        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertEqual("<lambda>", add_one._name)

        add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT(), name="add_one")
        self.assertEqual("add_one", add_one._name)

        subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT())
        self.assertEqual("SubtractOne", subtract_one._name)

        subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT(),
                           name="subtract_one")
        self.assertEqual("subtract_one", subtract_one._name)

        self.assertEqual("add", add._name)

        @udf(input_types=DataTypes.BIGINT(), result_type=DataTypes.BIGINT(), name="named")
        def named_udf(i):
            return i

        self.assertEqual("named", named_udf._name)

    def test_abc(self):
        class UdfWithoutEval(ScalarFunction):
            def open(self, function_context):
                pass

        with self.assertRaises(
                TypeError,
                msg="Can't instantiate abstract class UdfWithoutEval with abstract methods eval"):
            UdfWithoutEval()

    def test_invalid_udf(self):
        class Plus(object):
            def eval(self, col):
                return col + 1

        with self.assertRaises(
                TypeError,
                msg="Invalid function: not a function or callable (__call__ is not defined)"):
            # test non-callable function
            self.t_env.register_function(
                "non-callable-udf", udf(Plus(), DataTypes.BIGINT(), DataTypes.BIGINT()))

    def test_udf_without_arguments(self):
        self.t_env.register_function("one", udf(
            lambda: 1, input_types=[], result_type=DataTypes.BIGINT(), deterministic=True))
        self.t_env.register_function("two", udf(
            lambda: 2, input_types=[], result_type=DataTypes.BIGINT(), deterministic=False))

        table_sink = source_sink_utils.TestAppendSink(['a', 'b'],
                                                      [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select("one(), two()").insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,2", "1,2", "1,2"])


@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT())
def add(i, j):
    return i + j


class SubtractOne(ScalarFunction):

    def eval(self, i):
        return i - 1


class Subtract(ScalarFunction):

    def __init__(self):
        self.subtracted_value = 0

    def open(self, function_context):
        self.subtracted_value = 1

    def eval(self, i):
        return i - self.subtracted_value


class CallablePlus(object):

    def __call__(self, col):
        return col + 1


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
