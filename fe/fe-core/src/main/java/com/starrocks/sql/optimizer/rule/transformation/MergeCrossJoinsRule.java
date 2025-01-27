// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.tree.DataCachePopulateRewriteRule;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MergeCrossJoinsRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(DataCachePopulateRewriteRule.class);

    protected MergeCrossJoinsRule() {
        super(RuleType.TF_MERGE_CROSS_JOINS, Pattern.create(OperatorType.LOGICAL_JOIN));
    }

    private static final MergeCrossJoinsRule INSTANCE = new MergeCrossJoinsRule();

    public static MergeCrossJoinsRule getInstance() {
        return INSTANCE;
    }

    class JoinNode {
        OptExpression parent;
        OptExpression child;
        Integer childIndex;

        public JoinNode(OptExpression parent, OptExpression child, Integer childIndex) {
            this.parent = parent;
            this.child = child;
            this.childIndex = childIndex;
        }
    }

    private Optional<Long> getSourceTableId(OptExpression node) {
        Operator operator = node.getOp();
        if (operator instanceof LogicalScanOperator) {
            return Optional.of(((LogicalScanOperator) operator).getTable().getId());
        }
        if (operator instanceof LogicalJoinOperator) {
            return Optional.empty();
        }
        for (int i = 0; i < node.getInputs().size(); ++i) {
            OptExpression child = node.inputAt(i);
            Optional<Long> tableId = getSourceTableId(child);
            if (tableId.isPresent()) {
                return tableId;
            }
        }
        return Optional.empty();
    }

    private Optional<Pair<JoinNode, Integer>> findMainTable(OptExpression parent, int childIdx, OptExpression root,
                                                            Long mainTableId, Integer rootChildIdx) {
        Optional<Long> sourceTableId = getSourceTableId(root);
        if (sourceTableId.isPresent() && sourceTableId.get().equals(mainTableId)) {
            return Optional.of(Pair.create(new JoinNode(parent, root, childIdx), rootChildIdx));
        }
        for (int i = 0; i < root.getInputs().size(); ++i) {
            OptExpression child = root.inputAt(i);
            if (childIdx == -1) {
                rootChildIdx = i;
            }
            Optional<Pair<JoinNode, Integer>> newChild = findMainTable(root, i, child, mainTableId, rootChildIdx);
            if (newChild.isPresent()) {
                return newChild;
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        //        Operator inputOp = input.getOp();
        //
        //        boolean isInnerJoin =
        //                inputOp instanceof LogicalJoinOperator && ((LogicalJoinOperator) inputOp).getJoinType().isInnerJoin();
        //        if (!isInnerJoin) {
        //            return false;
        //        }
        //        boolean hasCrossJoin = input.getInputs().stream().anyMatch(child -> {
        //            Operator childOp = child.getOp();
        //            return (childOp instanceof LogicalJoinOperator) && ((LogicalJoinOperator) childOp).getJoinType().isCrossJoin();
        //        });
        //        if (!hasCrossJoin) {
        //            return false;
        //        }
        return super.check(input, context);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        Operator rootOp = input.getOp();
        Map<Long, HashSet<Long>> tableRelations = new HashMap<>();

        if (rootOp instanceof LogicalJoinOperator rootJoinOp) {
            if (rootJoinOp.getJoinType().isInnerJoin() && rootJoinOp.getOnPredicate() != null) {
                binaryRelation(rootJoinOp.getOnPredicate(), columnRefFactory, tableRelations);
            }
        }
        LOG.info("QQQQQQQQ: " + tableRelations.size());
        // all tables have a relationship with only the same table.
        // e.g. select * from t1, t2, t3 inner join t4 on t4.c1 = t1.c1 or t4.c1 = t2.c1 or t4.c1 = t3.c1
        Optional<Long> mainTableId = Optional.empty();
        for (Map.Entry<Long, HashSet<Long>> entry : tableRelations.entrySet()) {
            if (entry.getValue().size() > 1) {
                if (mainTableId.isPresent()) {
                    return Collections.emptyList();
                }
                mainTableId = Optional.of(entry.getKey());
            }
        }
        if (mainTableId.isPresent()) {
            Optional<Pair<JoinNode, Integer>> pair = findMainTable(null, -1, input, mainTableId.get(), null);
            if (pair.isPresent()) {
                JoinNode mainTableNode = pair.get().first;
                Integer rootChildIdx = pair.get().second;

                if (mainTableNode.childIndex != -1) {
                    OptExpression mainTableRoot = mainTableNode.child;
                    int mainTableChildIndex = mainTableNode.childIndex;
                    OptExpression mainTableParent = mainTableNode.parent;

                    int replaceChildIdx = rootChildIdx == 0 ? 1 : 0;
                    Operator replaceOp = input.getInputs().get(replaceChildIdx).getOp();
                    mainTableParent.setChild(mainTableChildIndex, OptExpression.create(replaceOp, mainTableRoot.getInputs()));

                    OptExpression newRoot = OptExpression.create(input.getOp(), input.getInputs());
                    newRoot.setChild(replaceChildIdx, mainTableRoot);
                    return List.of(newRoot);
                }
            }
        }
        return Collections.emptyList();
    }

    private static void binaryRelation(ScalarOperator onPredicate, ColumnRefFactory columnRefFactory,
                                       Map<Long, HashSet<Long>> tableRelations) {

        LOG.info("EEEEEEEE: " + columnRefFactory.getColumnRefToColumns().toString());
        LOG.info("EEEEEEEE: " + columnRefFactory.getColumnToRelationIds().toString());
        LOG.info("EEEEEEEE: " + columnRefFactory.getColumnRefToTable().toString());
        LOG.info("EEEEEEEE: " + onPredicate.toString());
        if (onPredicate instanceof CompoundPredicateOperator) {
            for (ScalarOperator scalarOperator : ((CompoundPredicateOperator) onPredicate).normalizeChildren()) {
                binaryRelation(scalarOperator, columnRefFactory, tableRelations);
            }
        } else if (onPredicate instanceof BinaryPredicateOperator) {
            int[] columnIds = onPredicate.getUsedColumns().getColumnIds();
            if (columnIds.length == 2) {
                int leftIdx = columnIds[0];
                int rightIdx = columnIds[1];

                Table leftTable = columnRefFactory.getTableForColumn(leftIdx);
                Table rightTable = columnRefFactory.getTableForColumn(rightIdx);

                if (rightTable != null && leftTable != null) {
                    tableRelations.computeIfAbsent(leftTable.getId(), k -> new HashSet<>()).add(rightTable.getId());
                    tableRelations.computeIfAbsent(rightTable.getId(), k -> new HashSet<>()).add(leftTable.getId());
                }
            }
        }
    }
}
