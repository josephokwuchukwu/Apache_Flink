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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.cost.FlinkCost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Flink bushy join reorder rule, which will convert {@link MultiJoin} to a bushy join tree.
 *
 * <p>In this bushy join reorder strategy, the join reorder step is as follows:
 *
 * <p>First step, we will reorder all the inner join type inputs in the multiJoin. We adopt the
 * concept of level in dynamic programming, and the latter layer will use the results stored in the
 * previous levels. First, we put all input factor (each input factor in {@link MultiJoin}) into
 * level 0, then we build all two-inputs join at level 1 based on the {@link FlinkCost} of level 0,
 * then we will build three-inputs join based on the previous two levels, then four-inputs joins ...
 * etc, util we reorder all the inner join type input factors in the multiJoin. When building
 * m-inputs join, we only keep the best plan (have the lowest {@link FlinkCost}) for the same set of
 * m inputs. E.g., for three-inputs join, we keep only the best plan for inputs {A, B, C} among
 * plans (A J B) J C, (A J C) J B, (B J C) J A.
 *
 * <p>Second step, we will add all outer join factors to the top of reordered join tree generated by
 * the first step. E.g., for the example (((A LJ B) IJ C) IJ D). we will first reorder A, C and D
 * using the first step strategy, get ((A IJ C) IJ D). Then, we will add B to the top, get (((A IJ
 * C) IJ D) LJ B).
 *
 * <p>Third step, we will add all cross join factors whose join condition is true to the top in the
 * final step.
 */
public class FlinkBushyJoinReorderRule extends RelRule<FlinkBushyJoinReorderRule.Config>
        implements TransformationRule {

    /** Creates a FlinkBushyJoinReorderRule. */
    protected FlinkBushyJoinReorderRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public FlinkBushyJoinReorderRule(RelBuilderFactory relBuilderFactory) {
        this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory).as(Config.class));
    }

    @Deprecated // to be removed before 2.0
    public FlinkBushyJoinReorderRule(
            RelFactories.JoinFactory joinFactory,
            RelFactories.ProjectFactory projectFactory,
            RelFactories.FilterFactory filterFactory) {
        this(RelBuilder.proto(joinFactory, projectFactory, filterFactory));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final RelBuilder relBuilder = call.builder();
        final MultiJoin multiJoinRel = call.rel(0);
        final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);
        RelNode bestOrder = findBestOrder(relBuilder, multiJoin);
        call.transformTo(bestOrder);
    }

    /**
     * Find best join reorder using bushy join reorder strategy. We will first try to reorder all
     * the inner join type input factors in the multiJoin. Then, we will add all outer join factors
     * to the top of reordered join tree generated by the first step. If there are factors, which
     * join condition is true, we will add these factors to the top in the final step.
     */
    private static RelNode findBestOrder(RelBuilder relBuilder, LoptMultiJoin multiJoin) {
        // Reorder all the inner join type input factors in the multiJoin.
        List<Map<Set<Integer>, JoinPlan>> foundPlansForInnerJoin =
                reorderInnerJoin(relBuilder, multiJoin);

        Map<Set<Integer>, JoinPlan> lastLevelOfInnerJoin =
                foundPlansForInnerJoin.get(foundPlansForInnerJoin.size() - 1);
        JoinPlan bestPlanForInnerJoin = getBestPlan(lastLevelOfInnerJoin);
        JoinPlan containOuterJoinPlan;
        // Add all outer join factors in the multiJoin (including left/right/full) on the
        // top of tree if outer join condition exists in multiJoin.
        if (multiJoin.getMultiJoinRel().isFullOuterJoin() || outerJoinConditionExists(multiJoin)) {
            containOuterJoinPlan = addOuterJoinToTop(bestPlanForInnerJoin, multiJoin, relBuilder);
        } else {
            containOuterJoinPlan = bestPlanForInnerJoin;
        }

        JoinPlan finalPlan;
        // Add all cross join factors whose join condition is true to the top.
        if (containOuterJoinPlan.factorIds.size() != multiJoin.getNumJoinFactors()) {
            finalPlan = addCrossJoinToTop(containOuterJoinPlan, multiJoin, relBuilder);
        } else {
            finalPlan = containOuterJoinPlan;
        }

        final List<String> fieldNames = multiJoin.getMultiJoinRel().getRowType().getFieldNames();
        return createTopProject(relBuilder, multiJoin, finalPlan, fieldNames);
    }

    /**
     * Reorder all the inner join type input factors in the multiJoin.
     *
     * <p>The result contains the selected join order of each layer and is stored in a HashMap. The
     * number of layers is equals to the number of inner join type input factors in the multiJoin.
     * E.g. for inner join case ((A IJ B) IJ C):
     *
     * <p>The stored HashMap of first layer in the result list is: [(Set(0), JoinPlan(Set(0), A)),
     * (Set(1), JoinPlan(Set(1), B)), (Set(2), JoinPlan(Set(2), C))].
     *
     * <p>The stored HashMap of second layer is [(Set(0, 1), JoinPlan(Set(0, 1), (A J B))), (Set(0,
     * 2), JoinPlan(Set(0, 2), (A J C))), (Set(1, 2), JoinPlan(Set(1, 2), (B J C)))].
     *
     * <p>The stored HashMap of third layer is [(Set(1, 0, 2), JoinPlan(Set(1, 0, 2), ((B J A) J
     * C)))].
     */
    private static List<Map<Set<Integer>, JoinPlan>> reorderInnerJoin(
            RelBuilder relBuilder, LoptMultiJoin multiJoin) {
        int numJoinFactors = multiJoin.getNumJoinFactors();
        List<Map<Set<Integer>, JoinPlan>> foundPlans = new ArrayList<>();

        // First, we put all join factors in MultiJoin into level 0.
        Map<Set<Integer>, JoinPlan> firstLevelJoinPlanMap = new LinkedHashMap<>();
        for (int i = 0; i < numJoinFactors; i++) {
            if (!multiJoin.isNullGenerating(i)) {
                Set<Integer> set1 = new HashSet<>();
                Set<Integer> set2 = new LinkedHashSet<>();
                set1.add(i);
                set2.add(i);
                RelNode joinFactor = multiJoin.getJoinFactor(i);
                firstLevelJoinPlanMap.put(set1, new JoinPlan(set2, joinFactor));
            }
        }
        foundPlans.add(firstLevelJoinPlanMap);

        // If multiJoin is full outer join, we will reorder it in method addOuterJoinToTop().
        if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
            return foundPlans;
        }

        // Build plans for next levels until the found plans size equals the number of join factors,
        // or no possible plan exists for next level.
        while (foundPlans.size() < numJoinFactors) {
            Map<Set<Integer>, JoinPlan> nextLevelJoinPlanMap =
                    foundNextLevel(relBuilder, new ArrayList<>(foundPlans), multiJoin);
            if (nextLevelJoinPlanMap.size() == 0) {
                break;
            }
            foundPlans.add(nextLevelJoinPlanMap);
        }

        return foundPlans;
    }

    private static boolean outerJoinConditionExists(LoptMultiJoin multiJoin) {
        for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
            if (multiJoin.getOuterJoinCond(i) != null
                    && RelOptUtil.conjunctions(multiJoin.getOuterJoinCond(i)).size() != 0) {
                return true;
            }
        }
        return false;
    }

    /** Get the best plan for current level by comparing cost. */
    private static JoinPlan getBestPlan(Map<Set<Integer>, JoinPlan> levelPlan) {
        JoinPlan bestPlan = null;
        for (Map.Entry<Set<Integer>, JoinPlan> entry : levelPlan.entrySet()) {
            if (bestPlan == null || entry.getValue().betterThan(bestPlan)) {
                bestPlan = entry.getValue();
            }
        }

        return bestPlan;
    }

    private static JoinPlan addOuterJoinToTop(
            JoinPlan bestPlan, LoptMultiJoin multiJoin, RelBuilder relBuilder) {
        List<Integer> remainIndexes =
                getRemainIndexes(multiJoin.getNumJoinFactors(), bestPlan.factorIds);
        RelNode leftNode = bestPlan.relNode;
        Set<Integer> set = new LinkedHashSet<>(bestPlan.factorIds);
        for (int index : remainIndexes) {
            RelNode rightNode = multiJoin.getJoinFactor(index);
            Optional<List<RexCall>> joinConditions =
                    getJoinConditions(
                            bestPlan.factorIds, Collections.singleton(index), multiJoin, true);

            if (!joinConditions.isPresent()) {
                // If the join condition doesn't exist, it means the join type is cross join, we
                // will add cross join to top in method addCrossJoinToTop() separately.
                continue;
            } else {
                List<RexCall> conditions = joinConditions.get();
                List<RexCall> newCondition =
                        convertToNewCondition(
                                new ArrayList<>(set),
                                Collections.singletonList(index),
                                conditions,
                                multiJoin);

                // For full outer join, we return the full join type. However, for left outer join
                // or right outer join, we will build a new join with left join type. For example,
                // for case (A RJ B) IJ C, we will return (B IJ C) LJ A.
                JoinRelType joinType = JoinRelType.LEFT;
                if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
                    assert remainIndexes.size() == 1;
                    joinType = JoinRelType.FULL;
                }
                relBuilder.clear();
                leftNode =
                        relBuilder
                                .push(leftNode)
                                .push(rightNode)
                                .join(joinType, newCondition)
                                .build();
            }
            set.add(index);
        }
        return new JoinPlan(set, leftNode);
    }

    private static JoinPlan addCrossJoinToTop(
            JoinPlan bestPlan, LoptMultiJoin multiJoin, RelBuilder relBuilder) {
        RexBuilder rexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
        List<Integer> remainIndexes =
                getRemainIndexes(multiJoin.getNumJoinFactors(), bestPlan.factorIds);

        RelNode leftNode = bestPlan.relNode;
        Set<Integer> set = new LinkedHashSet<>(bestPlan.factorIds);
        for (int index : remainIndexes) {
            relBuilder.clear();
            set.add(index);
            RelNode rightNode = multiJoin.getJoinFactor(index);
            leftNode =
                    relBuilder
                            .push(leftNode)
                            .push(rightNode)
                            .join(
                                    multiJoin.getMultiJoinRel().getJoinTypes().get(index),
                                    rexBuilder.makeLiteral(true))
                            .build();
        }
        return new JoinPlan(set, leftNode);
    }

    /**
     * Creates the topmost projection that will sit on top of the selected join ordering. The
     * projection needs to match the original join ordering. Also, places any post-join filters on
     * top of the project.
     */
    private static RelNode createTopProject(
            RelBuilder relBuilder,
            LoptMultiJoin multiJoin,
            JoinPlan finalPlan,
            List<String> fieldNames) {
        List<RexNode> newProjExprs = new ArrayList<>();
        RexBuilder rexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder();

        List<Integer> newJoinOrder = new ArrayList<>(finalPlan.factorIds);
        int nJoinFactors = multiJoin.getNumJoinFactors();
        List<RelDataTypeField> fields = multiJoin.getMultiJoinFields();

        // create a mapping from each factor to its field offset in the join
        // ordering
        final Map<Integer, Integer> factorToOffsetMap = new HashMap<>();
        for (int pos = 0, fieldStart = 0; pos < nJoinFactors; pos++) {
            factorToOffsetMap.put(newJoinOrder.get(pos), fieldStart);
            fieldStart += multiJoin.getNumFieldsInJoinFactor(newJoinOrder.get(pos));
        }

        for (int currFactor = 0; currFactor < nJoinFactors; currFactor++) {
            // if the factor is the right factor in a removable self-join,
            // then where possible, remap references to the right factor to
            // the corresponding reference in the left factor
            Integer leftFactor = null;
            if (multiJoin.isRightFactorInRemovableSelfJoin(currFactor)) {
                leftFactor = multiJoin.getOtherSelfJoinFactor(currFactor);
            }
            for (int fieldPos = 0;
                    fieldPos < multiJoin.getNumFieldsInJoinFactor(currFactor);
                    fieldPos++) {
                int newOffset =
                        requireNonNull(
                                        factorToOffsetMap.get(currFactor),
                                        () -> "factorToOffsetMap.get(currFactor)")
                                + fieldPos;
                if (leftFactor != null) {
                    Integer leftOffset = multiJoin.getRightColumnMapping(currFactor, fieldPos);
                    if (leftOffset != null) {
                        newOffset =
                                requireNonNull(
                                                factorToOffsetMap.get(leftFactor),
                                                "factorToOffsetMap.get(leftFactor)")
                                        + leftOffset;
                    }
                }
                newProjExprs.add(
                        rexBuilder.makeInputRef(
                                fields.get(newProjExprs.size()).getType(), newOffset));
            }
        }

        relBuilder.clear();
        relBuilder.push(finalPlan.relNode);
        relBuilder.project(newProjExprs, fieldNames);

        // Place the post-join filter (if it exists) on top of the final projection.
        RexNode postJoinFilter = multiJoin.getMultiJoinRel().getPostJoinFilter();
        if (postJoinFilter != null) {
            relBuilder.filter(postJoinFilter);
        }
        return relBuilder.build();
    }

    /** Found possible join plans for the next level based on the found plans in the prev levels. */
    private static Map<Set<Integer>, JoinPlan> foundNextLevel(
            RelBuilder relBuilder,
            List<Map<Set<Integer>, JoinPlan>> foundPlans,
            LoptMultiJoin multiJoin) {
        Map<Set<Integer>, JoinPlan> currentLevelJoinPlanMap = new LinkedHashMap<>();
        int foundPlansLevel = foundPlans.size() - 1;
        int joinLeftSideLevel = 0;
        int joinRightSideLevel = foundPlansLevel;
        while (joinLeftSideLevel <= joinRightSideLevel) {
            List<JoinPlan> joinLeftSidePlans =
                    new ArrayList<>(foundPlans.get(joinLeftSideLevel).values());
            int planSize = joinLeftSidePlans.size();
            for (int i = 0; i < planSize; i++) {
                JoinPlan joinLeftSidePlan = joinLeftSidePlans.get(i);
                List<JoinPlan> joinRightSidePlans;
                if (joinLeftSideLevel == joinRightSideLevel) {
                    // If left side level number equals right side level number. We can remove those
                    // top 'i' plans which already judged in right side plans to decrease search
                    // spaces.
                    joinRightSidePlans = new ArrayList<>(joinLeftSidePlans);
                    if (i > 0) {
                        joinRightSidePlans.subList(0, i).clear();
                    }
                } else {
                    joinRightSidePlans =
                            new ArrayList<>(foundPlans.get(joinRightSideLevel).values());
                }
                for (JoinPlan joinRightSidePlan : joinRightSidePlans) {
                    Optional<JoinPlan> newJoinPlan =
                            buildInnerJoin(
                                    relBuilder, joinLeftSidePlan, joinRightSidePlan, multiJoin);
                    if (newJoinPlan.isPresent()) {
                        JoinPlan existingPlanInCurrentLevel =
                                currentLevelJoinPlanMap.get(newJoinPlan.get().factorIds);
                        // check if it's the first plan for the factor set, or it's a better plan
                        // than the existing one due to lower cost.
                        if (existingPlanInCurrentLevel == null
                                || newJoinPlan.get().betterThan(existingPlanInCurrentLevel)) {
                            currentLevelJoinPlanMap.put(
                                    newJoinPlan.get().factorIds, newJoinPlan.get());
                        }
                    }
                }
            }
            joinLeftSideLevel++;
            joinRightSideLevel--;
        }
        return currentLevelJoinPlanMap;
    }

    private static Optional<JoinPlan> buildInnerJoin(
            RelBuilder relBuilder,
            JoinPlan leftSidePlan,
            JoinPlan rightSidePlan,
            LoptMultiJoin multiJoin) {
        // Intersect, should not join two overlapping factor sets.
        Set<Integer> resSet = new HashSet<>(leftSidePlan.factorIds);
        resSet.retainAll(rightSidePlan.factorIds);
        if (!resSet.isEmpty()) {
            return Optional.empty();
        }

        Optional<List<RexCall>> joinConditions =
                getJoinConditions(
                        leftSidePlan.factorIds, rightSidePlan.factorIds, multiJoin, false);
        if (!joinConditions.isPresent()) {
            return Optional.empty();
        }

        List<RexCall> conditions = joinConditions.get();
        Set<Integer> newFactorIds = new LinkedHashSet<>();
        JoinPlan newLeftSidePlan;
        JoinPlan newRightSidePlan;
        // put the deeper side on the left, tend to build a left-deep tree.
        if (leftSidePlan.factorIds.size() >= rightSidePlan.factorIds.size()) {
            newLeftSidePlan = leftSidePlan;
            newRightSidePlan = rightSidePlan;
        } else {
            newLeftSidePlan = rightSidePlan;
            newRightSidePlan = leftSidePlan;
        }
        newFactorIds.addAll(newLeftSidePlan.factorIds);
        newFactorIds.addAll(newRightSidePlan.factorIds);

        List<RexCall> newCondition =
                convertToNewCondition(
                        new ArrayList<>(newLeftSidePlan.factorIds),
                        new ArrayList<>(newRightSidePlan.factorIds),
                        conditions,
                        multiJoin);

        relBuilder.clear();
        Join newJoin =
                (Join)
                        relBuilder
                                .push(newLeftSidePlan.relNode)
                                .push(newRightSidePlan.relNode)
                                .join(JoinRelType.INNER, newCondition)
                                .build();

        return Optional.of(new JoinPlan(newFactorIds, newJoin));
    }

    private static List<RexCall> convertToNewCondition(
            List<Integer> leftFactorIds,
            List<Integer> rightFactorIds,
            List<RexCall> rexNodes,
            LoptMultiJoin multiJoin) {
        RexBuilder rexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
        List<RexCall> newCondition = new ArrayList<>();
        for (RexCall rexCond : rexNodes) {
            List<RexNode> resultRexNode = new ArrayList<>();
            for (RexNode rexNode : rexCond.getOperands()) {
                rexNode =
                        rexNode.accept(
                                new JoinConditionShuttle(multiJoin, leftFactorIds, rightFactorIds));
                resultRexNode.add(rexNode);
            }
            RexNode resultRex = rexBuilder.makeCall(rexCond.op, resultRexNode);
            newCondition.add((RexCall) resultRex);
        }

        return newCondition;
    }

    private static Optional<List<RexCall>> getJoinConditions(
            Set<Integer> leftSideFactorIds,
            Set<Integer> rightSideFactorIds,
            LoptMultiJoin multiJoin,
            boolean isOuterJoin) {
        List<RexCall> resultRexCall = new ArrayList<>();
        List<RexNode> joinConditions = new ArrayList<>();
        if (isOuterJoin && !multiJoin.getMultiJoinRel().isFullOuterJoin()) {
            for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
                joinConditions.addAll(RelOptUtil.conjunctions(multiJoin.getOuterJoinCond(i)));
            }
        } else {
            joinConditions = multiJoin.getJoinFilters();
        }

        for (RexNode joinCond : joinConditions) {
            if (joinCond instanceof RexCall) {
                RexCall callCondition = (RexCall) joinCond;
                ImmutableBitSet factorsRefByJoinFilter =
                        multiJoin.getFactorsRefByJoinFilter(callCondition);
                int leftSideFactorNumbers = 0;
                int rightSideFactorNumbers = 0;
                for (int leftSideFactorId : leftSideFactorIds) {
                    if (factorsRefByJoinFilter.get(leftSideFactorId)) {
                        leftSideFactorNumbers++;
                    }
                }
                for (int rightSideFactorId : rightSideFactorIds) {
                    if (factorsRefByJoinFilter.get(rightSideFactorId)) {
                        rightSideFactorNumbers++;
                    }
                }

                if (leftSideFactorNumbers > 0
                        && rightSideFactorNumbers > 0
                        && leftSideFactorNumbers + rightSideFactorNumbers
                                == factorsRefByJoinFilter.asSet().size()) {
                    resultRexCall.add(callCondition);
                }
            } else {
                return Optional.empty();
            }
        }

        if (resultRexCall.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(resultRexCall);
        }
    }

    private static List<Integer> getRemainIndexes(
            int totalNumOfJoinFactors, Set<Integer> factorIds) {
        List<Integer> remainIndexes = new ArrayList<>();
        for (int i = 0; i < totalNumOfJoinFactors; i++) {
            if (!factorIds.contains(i)) {
                remainIndexes.add(i);
            }
        }

        return remainIndexes;
    }

    // ~ Inner Classes ----------------------------------------------------------
    private static class JoinPlan {
        final Set<Integer> factorIds;
        final RelNode relNode;

        JoinPlan(Set<Integer> factorIds, RelNode relNode) {
            this.factorIds = factorIds;
            this.relNode = relNode;
        }

        private boolean betterThan(JoinPlan otherPlan) {
            RelMetadataQuery mq = this.relNode.getCluster().getMetadataQuery();
            RelOptCost thisCost = mq.getCumulativeCost(this.relNode);
            RelOptCost otherCost = mq.getCumulativeCost(otherPlan.relNode);
            if (thisCost == null || otherCost == null) {
                return false;
            } else {
                return thisCost.isLt(otherCost);
            }
        }
    }

    /**
     * This class is used to convert the rexInputRef index of the join condition before building a
     * new join.
     */
    private static class JoinConditionShuttle extends RexShuttle {
        private final LoptMultiJoin multiJoin;
        private final List<Integer> leftFactorIds;
        private final List<Integer> rightFactorIds;

        public JoinConditionShuttle(
                LoptMultiJoin multiJoin,
                List<Integer> leftFactorIds,
                List<Integer> rightFactorIds) {
            this.multiJoin = multiJoin;
            this.leftFactorIds = leftFactorIds;
            this.rightFactorIds = rightFactorIds;
        }

        @Override
        public RexNode visitInputRef(RexInputRef var) {
            int index = var.getIndex();
            int currentIndex = 0;
            int factorRef = multiJoin.findRef(index);
            if (leftFactorIds.contains(factorRef)) {
                for (Integer leftFactorId : leftFactorIds) {
                    if (leftFactorId == factorRef) {
                        currentIndex += findFactorIndex(index, multiJoin);
                        return new RexInputRef(currentIndex, var.getType());
                    } else {
                        currentIndex += multiJoin.getNumFieldsInJoinFactor(leftFactorId);
                    }
                }
            } else {
                for (int leftFactor : leftFactorIds) {
                    currentIndex += multiJoin.getNumFieldsInJoinFactor(leftFactor);
                }
                for (Integer rightFactorId : rightFactorIds) {
                    if (rightFactorId == factorRef) {
                        currentIndex += findFactorIndex(index, multiJoin);
                        return new RexInputRef(currentIndex, var.getType());
                    } else {
                        currentIndex += multiJoin.getNumFieldsInJoinFactor(rightFactorId);
                    }
                }
            }

            return var;
        }

        private static int findFactorIndex(int index, LoptMultiJoin multiJoin) {
            int factorId = multiJoin.findRef(index);
            int resultIndex = 0;
            for (int i = 0; i < factorId; i++) {
                resultIndex += multiJoin.getNumFieldsInJoinFactor(i);
            }
            return index - resultIndex;
        }
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                EMPTY.withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs())
                        .as(Config.class);

        @Override
        default FlinkBushyJoinReorderRule toRule() {
            return new FlinkBushyJoinReorderRule(this);
        }
    }
}
