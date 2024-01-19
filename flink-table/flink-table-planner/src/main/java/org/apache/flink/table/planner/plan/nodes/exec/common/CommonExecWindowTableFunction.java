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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.windowtvf.operator.AlignedWindowTableFunctionOperator;
import org.apache.flink.table.runtime.operators.window.windowtvf.operator.UnalignedWindowTableFunctionOperator;
import org.apache.flink.table.runtime.operators.window.windowtvf.operator.WindowTableFunctionOperatorBase;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZoneId;
import java.util.List;

import static org.apache.flink.table.planner.plan.utils.WindowTableFunctionUtil.createWindowAssigner;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Base {@link ExecNode} for window table-valued function. */
public abstract class CommonExecWindowTableFunction extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String WINDOW_TRANSFORMATION = "window";

    public static final String FIELD_NAME_WINDOWING = "windowing";

    @JsonProperty(FIELD_NAME_WINDOWING)
    protected final TimeAttributeWindowingStrategy windowingStrategy;

    protected CommonExecWindowTableFunction(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            TimeAttributeWindowingStrategy windowingStrategy,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.windowingStrategy = checkNotNull(windowingStrategy);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final boolean isAlignedWindow = windowingStrategy.getWindow().isAlignedWindow();
        if (isAlignedWindow) {
            return translateWithAlignedWindow(config, inputTransform);
        } else {
            return translateWithUnalignedWindow(
                    planner, config, (RowType) inputEdge.getOutputType(), inputTransform);
        }
    }

    private Transformation<RowData> translateWithAlignedWindow(
            ExecNodeConfig config, Transformation<RowData> inputTransform) {
        final WindowTableFunctionOperatorBase windowTableFunctionOperator =
                createAlignedWindowTableFunctionOperator(config);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(WINDOW_TRANSFORMATION, config),
                windowTableFunctionOperator,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                false);
    }

    private Transformation<RowData> translateWithUnalignedWindow(
            PlannerBase planner,
            ExecNodeConfig config,
            RowType inputRowType,
            Transformation<RowData> inputTransform) {
        final WindowTableFunctionOperatorBase windowTableFunctionOperator =
                createUnalignedWindowTableFunctionOperator(config, inputRowType);
        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(WINDOW_TRANSFORMATION, config),
                        windowTableFunctionOperator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);

        final int[] partitionKeys = extractPartitionKeys(windowingStrategy.getWindow());
        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        partitionKeys,
                        InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }

    private int[] extractPartitionKeys(WindowSpec window) {
        checkState(
                window instanceof SessionWindowSpec,
                "Only support unaligned window with session window now.");

        return ((SessionWindowSpec) window).getPartitionKeyIndices();
    }

    private WindowTableFunctionOperatorBase createAlignedWindowTableFunctionOperator(
            ExecNodeConfig config) {
        // TODO use WindowAssigner instead of using GroupWindowAssigner
        GroupWindowAssigner<TimeWindow> windowAssigner = createWindowAssigner(windowingStrategy);
        final ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        windowingStrategy.getTimeAttributeType(),
                        TableConfigUtils.getLocalTimeZone(config));
        return new AlignedWindowTableFunctionOperator(
                windowAssigner, windowingStrategy.getTimeAttributeIndex(), shiftTimeZone);
    }

    private WindowTableFunctionOperatorBase createUnalignedWindowTableFunctionOperator(
            ExecNodeConfig config, RowType inputRowType) {
        // TODO use WindowAssigner instead of using GroupWindowAssigner
        GroupWindowAssigner<TimeWindow> windowAssigner = createWindowAssigner(windowingStrategy);
        final ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        windowingStrategy.getTimeAttributeType(),
                        TableConfigUtils.getLocalTimeZone(config));

        return new UnalignedWindowTableFunctionOperator(
                windowAssigner,
                windowAssigner.getWindowSerializer(new ExecutionConfig()),
                inputRowType.getChildren().toArray(new LogicalType[0]),
                windowingStrategy.getTimeAttributeIndex(),
                shiftTimeZone);
    }
}
