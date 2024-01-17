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

package org.apache.flink.table.runtime.operators.aggregate.window.processors;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.windowtvf.unslicing.UnsliceAssigner;

import java.time.ZoneId;

/**
 * An window aggregate processor implementation which works for {@link UnsliceAssigner}, e.g.
 * session windows.
 */
public class UnsliceWindowAggProcessor extends AbstractUnsliceWindowAggProcessor {

    public UnsliceWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<TimeWindow> genAggsHandler,
            UnsliceAssigner<TimeWindow> unsliceAssigner,
            TypeSerializer<RowData> accSerializer,
            int indexOfCountStar,
            ZoneId shiftTimeZone) {
        super(genAggsHandler, unsliceAssigner, accSerializer, indexOfCountStar, shiftTimeZone);
    }
}
