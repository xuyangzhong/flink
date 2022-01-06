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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.logical.RowType;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.StringUtils.byteToHexString;

public class OperatorTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "operator";

    public static final ConfigOption<String> DIGEST =
            ConfigOptions.key("digest").stringType().noDefaultValue();

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Map<String, String> properties = context.getCatalogTable().getOptions();
        String endpoint = properties.getOrDefault("endpoint", "http://localhost:8080");
        String jobId = properties.get("job_id");
        int parallelism = Integer.valueOf(properties.getOrDefault("parallelism", "1"));
        RowType rowType = (RowType) context.getPhysicalRowDataType().getLogicalType();
        RuntimeExecutionMode mode = context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE);
        boolean isBounded = mode.equals(RuntimeExecutionMode.BATCH);
        return new OperatorTableSource(
                endpoint,
                jobId,
                fromUID(context.getObjectIdentifier().getObjectName()),
                parallelism,
                rowType,
                isBounded);
    }

    private static String fromUID(String headOpUid) {
        final HashFunction hashFunction = Hashing.murmur3_128(0);
        final Charset charset = StandardCharsets.UTF_8;
        byte[] bytes = hashFunction.newHasher().putString(headOpUid, charset).hash().asBytes();
        return byteToHexString(bytes, 0, bytes.length);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DIGEST);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
