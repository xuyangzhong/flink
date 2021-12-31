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

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;

public class ReuseExecNodeGraphProcessor implements ExecNodeGraphProcessor {

    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_REUSE_OPERATOR_ENABLED =
            key("table.optimizer.reuse-operator-enabled").defaultValue(true).withDescription("");

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        if (!context.getPlanner()
                .getTableConfig()
                .getConfiguration()
                .get(TABLE_OPTIMIZER_REUSE_OPERATOR_ENABLED)) {
            return execGraph;
        }
        try {
            return doProcess(execGraph, context);
        } catch (Exception e) {
            throw new TableException(e.getMessage());
        }
    }

    private ExecNodeGraph doProcess(ExecNodeGraph execGraph, ProcessorContext context)
            throws Exception {
        Map<String, CatalogBaseTable> operatorTableMap = buildOperatorTableMap(context);
        if (operatorTableMap.isEmpty()) {
            return execGraph;
        }
        ExecNodeVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        List<ExecEdge> newEdges = new ArrayList<>();
                        for (ExecEdge e : node.getInputEdges()) {
                            CatalogBaseTable table =
                                    operatorTableMap.get(e.getSource().getDigest());
                            if (table != null) {
                                ExecNode<?> newInput =
                                        createTableSourceScan(
                                                context.getPlanner(),
                                                table,
                                                (RowType) e.getOutputType());
                                newEdges.add(
                                        new ExecEdge(
                                                newInput,
                                                e.getTarget(),
                                                e.getShuffle(),
                                                e.getExchangeMode()));
                            } else {
                                newEdges.add(e);
                                e.getSource().accept(this);
                            }
                        }
                        node.setInputEdges(newEdges);
                    }
                };
        execGraph.getRootNodes().forEach(n -> n.accept(visitor));
        return execGraph;
    }

    private StreamExecTableSourceScan createTableSourceScan(
            PlannerBase planner, CatalogBaseTable table, RowType outputType) {
        ObjectIdentifier identifier =
                ObjectIdentifier.of(
                        planner.catalogManager().getCurrentCatalog(),
                        table.getOptions().get("job_id"),
                        table.getOptions().get("op_id"));
        ResolvedCatalogTable resolvedCatalogTable =
                planner.catalogManager().resolveCatalogTable((CatalogTable) table);
        DynamicTableSourceSpec spec =
                new DynamicTableSourceSpec(identifier, resolvedCatalogTable, new ArrayList<>());
        spec.setReadableConfig(planner.getTableConfig().getConfiguration());
        spec.setClassLoader(planner.createSerdeContext().getClassLoader());
        StreamExecTableSourceScan scan =
                new StreamExecTableSourceScan(spec, outputType, identifier.toString());
        scan.setInputEdges(new ArrayList<>());
        return scan;
    }

    private Map<String, CatalogBaseTable> buildOperatorTableMap(ProcessorContext context)
            throws Exception {
        CatalogManager catalogManager = context.getPlanner().catalogManager();
        String currentCatalog = catalogManager.getCurrentCatalog();
        Catalog catalog = catalogManager.getCatalog(currentCatalog).get();
        List<String> jobIds =
                catalog.listDatabases().stream()
                        .filter(
                                dbName -> {
                                    try {
                                        CatalogDatabase database = catalog.getDatabase(dbName);
                                        return database.getProperties()
                                                .getOrDefault("type", "")
                                                .equals("job");
                                    } catch (DatabaseNotExistException e) {
                                        throw new TableException(e.getMessage());
                                    }
                                })
                        .collect(Collectors.toList());

        Map<String, CatalogBaseTable> operatorTables = new HashMap<>();
        for (String jobId : jobIds) {
            List<String> operators = catalog.listTables(jobId);
            for (String operator : operators) {
                CatalogBaseTable table = catalog.getTable(new ObjectPath(jobId, operator));
                String digest = table.getOptions().get("digest");
                operatorTables.put(digest, table);
            }
        }
        return operatorTables;
    }
}
