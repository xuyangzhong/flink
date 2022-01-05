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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.collect.AbstractKeyedProcessOperatorWithCollector;
import org.apache.flink.table.runtime.operators.collect.Scannable;
import org.apache.flink.table.runtime.operators.collect.TableCollectSinkFunction;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/** GroupAggregateOperator. */
public class GroupAggregateOperator
        extends AbstractKeyedProcessOperatorWithCollector<RowData, RowData, RowData>
        implements Scannable<RowData> {

    private static final long serialVersionUID = 1L;

    public GroupAggregateOperator(GroupAggFunction function) {
        super(function);
    }

    @Override
    public void scan(TableCollectSinkFunction<RowData> sink, long id) {
        LOG.info("start scan for subscriber " + id);
        Stream<Object> keyIterator =
                getKeyedStateBackend().getKeys("accState", VoidNamespace.INSTANCE);
        AtomicInteger count = new AtomicInteger(0);
        keyIterator.forEach(
                key -> {
                    setCurrentKey(key);
                    try {
                        RowData value =
                                ((GroupAggFunction) getUserFunction())
                                        .getCurrentValue((RowData) key);
                        if (value != null) {
                            sink.invoke(value, id);
                            count.getAndIncrement();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        LOG.info("end scan for subscriber " + id + " row: " + count.get());
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {}
}
