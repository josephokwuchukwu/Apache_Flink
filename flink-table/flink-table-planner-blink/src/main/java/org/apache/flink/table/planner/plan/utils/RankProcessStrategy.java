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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram;
import org.apache.flink.table.planner.plan.trait.RelModifiedMonotonicity;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/** Base class of Strategy to choose different rank process function. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
        value = {
            @JsonSubTypes.Type(
                    value = RankProcessStrategy.UndefinedStrategy.class,
                    name = "Undefined"),
            @JsonSubTypes.Type(
                    value = RankProcessStrategy.AppendFastStrategy.class,
                    name = "AppendFast"),
            @JsonSubTypes.Type(value = RankProcessStrategy.RetractStrategy.class, name = "Retract"),
            @JsonSubTypes.Type(
                    value = RankProcessStrategy.UpdateFastStrategy.class,
                    name = "UpdateFast"),
        })
public interface RankProcessStrategy {

    String FIELD_NAME_TYPE = "type";

    String getType();

    UndefinedStrategy UNDEFINED_STRATEGY = new UndefinedStrategy();

    AppendFastStrategy APPEND_FAST_STRATEGY = new AppendFastStrategy();

    RetractStrategy RETRACT_STRATEGY = new RetractStrategy();

    /**
     * A placeholder strategy which will be inferred after {@link
     * FlinkChangelogModeInferenceProgram}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    class UndefinedStrategy implements RankProcessStrategy {
        @JsonCreator
        public UndefinedStrategy() {}

        @Override
        public String toString() {
            return "UndefinedStrategy";
        }

        @Override
        @JsonProperty(FIELD_NAME_TYPE)
        public String getType() {
            return "Undefined";
        }
    }

    /** A strategy which only works when input only contains insertion changes. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    class AppendFastStrategy implements RankProcessStrategy {
        @JsonCreator
        public AppendFastStrategy() {}

        @Override
        public String toString() {
            return "AppendFastStrategy";
        }

        @Override
        @JsonProperty(FIELD_NAME_TYPE)
        public String getType() {
            return "AppendFast";
        }
    }

    /** A strategy which works when input contains update or deletion changes. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    class RetractStrategy implements RankProcessStrategy {
        @JsonCreator
        public RetractStrategy() {}

        @Override
        public String toString() {
            return "RetractStrategy";
        }

        @Override
        @JsonProperty(FIELD_NAME_TYPE)
        public String getType() {
            return "Retract";
        }
    }

    /**
     * A strategy which only works when input shouldn't contains deletion changes and input should
     * have the given {@link #primaryKeys} and should be monotonic on the order by field.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    class UpdateFastStrategy implements RankProcessStrategy {

        public static final String FIELD_NAME_PRIMARY_KEYS = "primaryKeys";

        @JsonProperty(FIELD_NAME_PRIMARY_KEYS)
        private final int[] primaryKeys;

        @JsonCreator
        public UpdateFastStrategy(@JsonProperty(FIELD_NAME_PRIMARY_KEYS) int[] primaryKeys) {
            this.primaryKeys = primaryKeys;
        }

        @JsonIgnore
        public int[] getPrimaryKeys() {
            return primaryKeys;
        }

        @Override
        public String toString() {
            return String.format("UpdateFastStrategy[%s]", StringUtils.join(primaryKeys, ','));
        }

        @Override
        @JsonProperty(FIELD_NAME_TYPE)
        public String getType() {
            return "UpdateFast";
        }
    }

    /** Gets {@link RankProcessStrategy} based on input, partitionKey and orderKey. */
    static List<RankProcessStrategy> analyzeRankProcessStrategies(
            StreamPhysicalRel rank, ImmutableBitSet partitionKey, RelCollation orderKey) {

        RelMetadataQuery mq = rank.getCluster().getMetadataQuery();
        List<RelFieldCollation> fieldCollations = orderKey.getFieldCollations();
        boolean isUpdateStream = !ChangelogPlanUtils.inputInsertOnly(rank);
        RelNode input = rank.getInput(0);

        if (isUpdateStream) {
            Set<ImmutableBitSet> uniqueKeys = mq.getUniqueKeys(input);
            if (uniqueKeys == null
                    || uniqueKeys.isEmpty()
                    // unique key should contains partition key
                    || uniqueKeys.stream().noneMatch(k -> k.contains(partitionKey))) {
                // and we fall back to using retract rank
                return Collections.singletonList(RETRACT_STRATEGY);
            } else {
                FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
                RelModifiedMonotonicity monotonicity = fmq.getRelModifiedMonotonicity(input);
                boolean isMonotonic = false;
                if (monotonicity != null && !fieldCollations.isEmpty()) {
                    isMonotonic =
                            fieldCollations.stream()
                                    .allMatch(
                                            collation -> {
                                                SqlMonotonicity fieldMonotonicity =
                                                        monotonicity
                                                                .fieldMonotonicities()[
                                                                collation.getFieldIndex()];
                                                RelFieldCollation.Direction direction =
                                                        collation.direction;
                                                if ((fieldMonotonicity == SqlMonotonicity.DECREASING
                                                                || fieldMonotonicity
                                                                        == SqlMonotonicity
                                                                                .STRICTLY_DECREASING)
                                                        && direction
                                                                == RelFieldCollation.Direction
                                                                        .ASCENDING) {
                                                    // sort field is ascending and its monotonicity
                                                    // is decreasing
                                                    return true;
                                                } else if ((fieldMonotonicity
                                                                        == SqlMonotonicity
                                                                                .INCREASING
                                                                || fieldMonotonicity
                                                                        == SqlMonotonicity
                                                                                .STRICTLY_INCREASING)
                                                        && direction
                                                                == RelFieldCollation.Direction
                                                                        .DESCENDING) {
                                                    // sort field is descending and its monotonicity
                                                    // is increasing
                                                    return true;
                                                } else {
                                                    // sort key is a grouping key of upstream agg,
                                                    // it is monotonic
                                                    return fieldMonotonicity
                                                            == SqlMonotonicity.CONSTANT;
                                                }
                                            });
                }

                if (isMonotonic) {
                    // TODO: choose a set of primary key
                    return Arrays.asList(
                            new UpdateFastStrategy(uniqueKeys.iterator().next().toArray()),
                            RETRACT_STRATEGY);
                } else {
                    return Collections.singletonList(RETRACT_STRATEGY);
                }
            }
        } else {
            return Collections.singletonList(APPEND_FAST_STRATEGY);
        }
    }
}
