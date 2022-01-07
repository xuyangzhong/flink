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
 *
 */

package org.apache.flink.table.planner.connectors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connectors.mv.OperatorOutputLookup;
import org.apache.flink.table.connectors.mv.OperatorOutputSubscriber;
import org.apache.flink.table.connectors.mv.SerdeUtil;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** OperatorTableSource. */
public class OperatorTableSource
        implements ScanTableSource, LookupTableSource, SupportsFilterPushDown {
    private static final Logger LOG = LoggerFactory.getLogger(OperatorTableSource.class);

    private final String endpoint;
    private final String jobId;
    private final String operatorId;
    private final RowType rowType;
    private final int parallelism;
    private final boolean isBounded;
    private final ChangelogMode changelogMode;
    private final @Nullable RowType keyType;
    private @Nullable byte[] key;

    public OperatorTableSource(
            String endpoint,
            String jobId,
            String operatorId,
            int parallelism,
            RowType rowType,
            boolean isBounded,
            ChangelogMode changelogMode,
            @Nullable RowType keyType) {
        this(
                endpoint,
                jobId,
                operatorId,
                parallelism,
                rowType,
                isBounded,
                changelogMode,
                keyType,
                null);
    }

    private OperatorTableSource(
            String endpoint,
            String jobId,
            String operatorId,
            int parallelism,
            RowType rowType,
            boolean isBounded,
            ChangelogMode changelogMode,
            @Nullable RowType keyType,
            @Nullable byte[] key) {
        this.endpoint = endpoint;
        this.jobId = jobId;
        this.rowType = rowType;
        this.operatorId = operatorId;
        this.parallelism = parallelism;
        this.isBounded = isBounded;
        this.changelogMode = changelogMode;
        this.keyType = keyType;
        this.key = key;
    }

    @Override
    public DynamicTableSource copy() {
        return new OperatorTableSource(
                endpoint,
                jobId,
                operatorId,
                parallelism,
                rowType,
                isBounded,
                changelogMode,
                keyType,
                key);
    }

    @Override
    public String asSummaryString() {
        return "operator source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (isBounded) {
            return ChangelogMode.insertOnly();
        }
        return changelogMode;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        LOG.info(
                "use ScanRuntimeProvider: jobId={}, operatorId={} parallelism={}, isBounded={}, key={}, keyType={}",
                jobId,
                operatorId,
                parallelism,
                isBounded,
                key,
                keyType);
        return SourceFunctionProvider.of(
                new OperatorOutputSubscriber(
                        endpoint, jobId, operatorId, parallelism, rowType, isBounded, key, keyType),
                isBounded);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        if (keyType == null) {
            throw new TableException(
                    String.format(
                            "%s does not defined key, so does not support LookupTableSource",
                            operatorId));
        }
        LOG.info(
                "use LookupRuntimeProvider: jobId={}, operatorId={} parallelism={}, key={}, keyType={}",
                jobId,
                operatorId,
                parallelism,
                key,
                keyType);

        return TableFunctionProvider.of(
                // only lookup the latest one
                new OperatorOutputLookup(
                        endpoint, jobId, operatorId, parallelism, rowType, keyType));
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        LOG.info("try push filters={}, keyType={}", filters, keyType);
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();
        if (keyType != null) {
            Map<String, Object> map = new HashMap<>();
            List<String> keyFields = keyType.getFieldNames();
            for (ResolvedExpression filter : filters) {
                Tuple2<String, Object> nameAndValue = canPushDown(filter, keyFields);
                if (nameAndValue != null) {
                    map.put(nameAndValue.f0, nameAndValue.f1);
                    acceptedFilters.add(filter);
                } else {
                    remainingFilters.add(filter);
                }
            }
            if (map.keySet().equals(new HashSet<>(keyFields))) {
                LOG.info("apply filter push down");
                Object[] value = keyFields.stream().map(map::get).toArray();
                try {
                    key = SerdeUtil.serialize(value, keyType);
                } catch (IOException e) {
                    LOG.info("Failed to serialize key");
                    remainingFilters.clear();
                    remainingFilters.addAll(filters);
                }
            } else {
                remainingFilters.clear();
                remainingFilters.addAll(filters);
            }
        } else {
            remainingFilters.addAll(filters);
        }
        return Result.of(acceptedFilters, remainingFilters);
    }

    private Tuple2<String, Object> canPushDown(
            ResolvedExpression expr, List<String> filterableFields) {
        if (expr instanceof CallExpression
                && expr.getChildren().size() == 2
                && ((CallExpression) expr).getFunctionDefinition()
                        == BuiltInFunctionDefinitions.EQUALS) {
            ResolvedExpression first = expr.getResolvedChildren().get(0);
            ResolvedExpression second = expr.getResolvedChildren().get(1);
            if (first instanceof FieldReferenceExpression
                    && second instanceof ValueLiteralExpression) {
                return getNameAndValue(
                        (FieldReferenceExpression) first,
                        (ValueLiteralExpression) second,
                        filterableFields);
            }
            if (second instanceof FieldReferenceExpression
                    && first instanceof ValueLiteralExpression) {
                return getNameAndValue(
                        (FieldReferenceExpression) second,
                        (ValueLiteralExpression) first,
                        filterableFields);
            }
        }
        return null;
    }

    private Tuple2<String, Object> getNameAndValue(
            FieldReferenceExpression ref,
            ValueLiteralExpression value,
            List<String> filterableFields) {
        String name = ref.getName();
        if (filterableFields.contains(name)) {
            Optional<?> opt = value.getValueAs(value.getOutputDataType().getConversionClass());
            if (opt.isPresent()) {
                return new Tuple2<>(name, opt.get());
            }
        }
        return null;
    }
}
