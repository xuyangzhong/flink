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

package org.apache.flink.table.runtime.operators.collect;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapping {@link Output} that updates metrics on the number of emitted elements for collecting.
 */
public class TableCollectOutput<OUT> implements Output<StreamRecord<OUT>> {
    private static final Logger LOG = LoggerFactory.getLogger(TableCollectSinkFunction.class);

    private final Output<StreamRecord<OUT>> output;
    private final TableCollectSinkFunction<OUT> collectFunc;
    private boolean collectEnabled;

    public TableCollectOutput(
            Output<StreamRecord<OUT>> output, TableCollectSinkFunction<OUT> collectFunc) {
        this.output = output;
        this.collectFunc = collectFunc;
    }

    public void setCollectEnabled(boolean isEnabled) {
        collectEnabled = isEnabled;
    }

    @Override
    public void emitWatermark(Watermark mark) {
        output.emitWatermark(mark);
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        output.emitWatermarkStatus(watermarkStatus);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        output.emitLatencyMarker(latencyMarker);
    }

    @Override
    public void collect(StreamRecord<OUT> record) {
        output.collect(record);
        if (collectFunc == null || !collectEnabled) {
            return;
        }
        try {
            collectFunc.invoke(record.getValue());
        } catch (Exception e) {
            LOG.error("Invoking common collect function failed", e);
        }
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        output.collect(outputTag, record);
    }

    @Override
    public void close() {
        output.close();
    }
}
