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

package org.apache.flink.table.utils;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ReuseExecNodeGraphProcessor;

import java.util.List;

public class Test {

    public static void main(String[] args) throws Exception {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        env.getConfig()
                .getConfiguration()
                .setBoolean(
                        ReuseExecNodeGraphProcessor.TABLE_OPTIMIZER_REUSE_OPERATOR_ENABLED, true);
        env.executeSql(
                "create table my_table(a int, b bigint, c varchar) with ('connector' = 'datagen')");
        env.executeSql(
                "create table my_sink(a int, b bigint, c varchar) with ('connector' = 'blackhole')");
        System.out.println(
                env.explainSql(
                        "insert into my_sink select a, count(b) as b, max(c) as c from my_table group by a"));

        Catalog catalog = env.getCatalog(env.getCurrentCatalog()).get();
        List<String> databases = catalog.listDatabases();
        String jobId =
                databases.stream()
                        .filter(d -> !env.getCurrentDatabase().equals(d))
                        .findFirst()
                        .get();
        System.out.println("job id: " + jobId);
        List<String> operators = catalog.listTables(jobId);
        System.out.println("operators: " + operators);

        System.out.println(
                env.explainSql(String.format("select * from `%s`.`%s`", jobId, operators.get(0))));

        System.out.println(
                env.explainSql(
                        "insert into my_sink select * from "
                                + "(select a, count(b) as b, max(c) as c from my_table group by a) where b > 10"));
    }
}
