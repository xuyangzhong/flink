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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleInputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOutputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.commoncollect.CommonCollectOperatorFactory;
import org.apache.flink.streaming.api.operators.commoncollect.CommonCollectible;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** An Utility class that helps translating {@link ExecNode} to {@link Transformation}. */
public class ExecNodeUtil {
    /**
     * Sets {Transformation#declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase, int)}
     * using the given bytes for {@link ManagedMemoryUseCase#OPERATOR}.
     */
    public static <T> void setManagedMemoryWeight(
            Transformation<T> transformation, long memoryBytes) {
        if (memoryBytes > 0) {
            final int weightInMebibyte = Math.max(1, (int) (memoryBytes >> 20));
            final Optional<Integer> previousWeight =
                    transformation.declareManagedMemoryUseCaseAtOperatorScope(
                            ManagedMemoryUseCase.OPERATOR, weightInMebibyte);
            if (previousWeight.isPresent()) {
                throw new TableException(
                        "Managed memory weight has been set, this should not happen.");
            }
        }
    }

    /** Create a {@link OneInputTransformation}. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String desc,
            StreamOperator<O> operator,
            TypeInformation<O> outputType,
            int parallelism) {
        return createOneInputTransformation(
                input, name, desc, operator, outputType, parallelism, 0);
    }

    /** Create a {@link OneInputTransformation} with memoryBytes. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String desc,
            StreamOperator<O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        StreamOperatorFactory factory = null;
        if (operator == null) {
            factory = null;
        } else if (operator instanceof StreamSource
                && ((StreamSource) operator).getUserFunction()
                        instanceof InputFormatSourceFunction) {
            factory = new SimpleInputFormatOperatorFactory((StreamSource) operator);
        } else if (operator instanceof StreamSink
                && ((StreamSink) operator).getUserFunction() instanceof OutputFormatSinkFunction) {
            factory = new SimpleOutputFormatOperatorFactory((StreamSink) operator);
        } else if (operator instanceof AbstractStreamOperator
                && operator instanceof CommonCollectible) {
            TypeSerializer<O> serializer = outputType.createSerializer(new ExecutionConfig());
            factory =
                    new CommonCollectOperatorFactory(
                            serializer, (AbstractStreamOperator<O>) operator);
        } else {
            factory = new SimpleOperatorFactory(operator);
        }

        return createOneInputTransformation(
                input, name, desc, factory, outputType, parallelism, memoryBytes);
    }

    /** Create a {@link OneInputTransformation}. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String desc,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism) {
        return createOneInputTransformation(
                input, name, desc, operatorFactory, outputType, parallelism, 0);
    }

    /** Create a {@link OneInputTransformation} with memoryBytes. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String desc,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        OneInputTransformation<I, O> transformation =
                new OneInputTransformation<>(input, name, operatorFactory, outputType, parallelism);
        setManagedMemoryWeight(transformation, memoryBytes);
        transformation.setDescription(desc);
        return transformation;
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <IN1, IN2, O> TwoInputTransformation<IN1, IN2, O> createTwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            String name,
            String desc,
            TwoInputStreamOperator<IN1, IN2, O> operator,
            TypeInformation<O> outputType,
            int parallelism) {
        return createTwoInputTransformation(
                input1, input2, name, desc, operator, outputType, parallelism, 0);
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <IN1, IN2, O> TwoInputTransformation<IN1, IN2, O> createTwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            String name,
            String desc,
            TwoInputStreamOperator<IN1, IN2, O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        return createTwoInputTransformation(
                input1,
                input2,
                name,
                desc,
                SimpleOperatorFactory.of(operator),
                outputType,
                parallelism,
                memoryBytes);
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <I1, I2, O> TwoInputTransformation<I1, I2, O> createTwoInputTransformation(
            Transformation<I1> input1,
            Transformation<I2> input2,
            String name,
            String desc,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        TwoInputTransformation<I1, I2, O> transformation =
                new TwoInputTransformation<>(
                        input1, input2, name, operatorFactory, outputType, parallelism);
        setManagedMemoryWeight(transformation, memoryBytes);
        transformation.setDescription(desc);
        return transformation;
    }

    /** Return description for multiple input node. */
    public static String getMultipleInputDescription(
            ExecNode<?> rootNode,
            List<ExecNode<?>> inputNodes,
            List<InputProperty> inputProperties) {
        String members =
                ExecNodePlanDumper.treeToString(rootNode, inputNodes, true).replace("\n", "\\n");
        StringBuilder sb = new StringBuilder();
        sb.append("MultipleInput(");
        List<String> readOrders =
                inputProperties.stream()
                        .map(InputProperty::getPriority)
                        .map(Object::toString)
                        .collect(Collectors.toList());
        boolean hasDiffReadOrder = readOrders.stream().distinct().count() > 1;
        if (hasDiffReadOrder) {
            sb.append("readOrder=[").append(String.join(",", readOrders)).append("], ");
        }
        sb.append("members=[\\n").append(members).append("]");
        sb.append(")");
        return sb.toString();
    }

    /**
     * The planner might have more information than expressed in legacy source transformations. This
     * enforces planner information about boundedness to the affected transformations.
     */
    public static void makeLegacySourceTransformationsBounded(Transformation<?> transformation) {
        if (transformation instanceof LegacySourceTransformation) {
            ((LegacySourceTransformation<?>) transformation).setBoundedness(Boundedness.BOUNDED);
        }
        transformation.getInputs().forEach(ExecNodeUtil::makeLegacySourceTransformationsBounded);
    }
}
