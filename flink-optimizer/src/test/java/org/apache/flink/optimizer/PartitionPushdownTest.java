/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.optimizer;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@SuppressWarnings("serial")
public class PartitionPushdownTest extends CompilerTestBase {

    @Test
    void testPartitioningNotPushedDown() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            @SuppressWarnings("unchecked")
            DataSet<Tuple3<Long, Long, Long>> input =
                    env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

            input.groupBy(0, 1)
                    .sum(2)
                    .groupBy(0)
                    .sum(1)
                    .output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();

            SingleInputPlanNode agg2Reducer = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode agg2Combiner =
                    (SingleInputPlanNode) agg2Reducer.getInput().getSource();
            SingleInputPlanNode agg1Reducer =
                    (SingleInputPlanNode) agg2Combiner.getInput().getSource();

            assertThat(agg2Reducer.getInput().getShipStrategy())
                    .isEqualTo(ShipStrategyType.PARTITION_HASH);
            assertThat(agg2Reducer.getInput().getShipStrategyKeys()).isEqualTo(new FieldList(0));

            assertThat(agg2Combiner.getInput().getShipStrategy())
                    .isEqualTo(ShipStrategyType.FORWARD);

            assertThat(agg1Reducer.getInput().getShipStrategy())
                    .isEqualTo(ShipStrategyType.PARTITION_HASH);
            assertThat(agg1Reducer.getInput().getShipStrategyKeys()).isEqualTo(new FieldList(0, 1));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testPartitioningReused() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            @SuppressWarnings("unchecked")
            DataSet<Tuple3<Long, Long, Long>> input =
                    env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

            input.groupBy(0)
                    .sum(1)
                    .groupBy(0, 1)
                    .sum(2)
                    .output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();

            SingleInputPlanNode agg2Reducer = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode agg1Reducer =
                    (SingleInputPlanNode) agg2Reducer.getInput().getSource();

            assertThat(agg2Reducer.getInput().getShipStrategy())
                    .isEqualTo(ShipStrategyType.FORWARD);

            assertThat(agg1Reducer.getInput().getShipStrategy())
                    .isEqualTo(ShipStrategyType.PARTITION_HASH);
            assertThat(agg1Reducer.getInput().getShipStrategyKeys()).isEqualTo(new FieldList(0));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
