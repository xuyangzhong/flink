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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of a {@link CatalogTable}. */
@Internal
public class CatalogOpTable implements CatalogTable {

    private final Schema schema;
    private final int id;
    private final Map<String, String> options;
    private final String comment;

    public CatalogOpTable(Schema schema, int id, Map<String, String> options, String comment) {
        this.schema = checkNotNull(schema, "Schema must not be null.");
        this.id = id;
        this.options = checkNotNull(options, "Options must not be null.");
        this.comment = comment;

        checkArgument(
                options.entrySet().stream()
                        .allMatch(e -> e.getKey() != null && e.getValue() != null),
                "Options cannot have null keys or values.");
    }

    @Override
    public Schema getUnresolvedSchema() {
        return schema;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    @Override
    public List<String> getPartitionKeys() {
        return new ArrayList<>();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public CatalogBaseTable copy() {
        return new CatalogOpTable(schema, id, options, comment);
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new CatalogOpTable(schema, id, options, comment);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Map<String, String> toProperties() {
        throw new UnsupportedOperationException(
                "Only a resolved catalog table can be serialized into a map of string properties.");
    }
}
