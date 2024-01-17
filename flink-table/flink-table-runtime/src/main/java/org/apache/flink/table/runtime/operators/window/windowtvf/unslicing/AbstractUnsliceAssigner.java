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

package org.apache.flink.table.runtime.operators.window.windowtvf.unslicing;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.groupwindow.internal.MergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.windowtvf.common.ClockService;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Optional;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;
import static org.apache.flink.util.Preconditions.checkState;

/** A base implementation of {@link UnsliceAssigner}. */
public abstract class AbstractUnsliceAssigner<W extends Window> implements UnsliceAssigner<W> {

    protected final int rowtimeIndex;
    protected final boolean isEventTime;
    protected final ZoneId shiftTimeZone;

    public AbstractUnsliceAssigner(int rowtimeIndex, ZoneId shiftTimeZone) {
        this.rowtimeIndex = rowtimeIndex;
        this.shiftTimeZone = shiftTimeZone;
        this.isEventTime = rowtimeIndex >= 0;
    }

    @Override
    public Optional<W> assignActualWindow(
            RowData element, ClockService clock, MergingWindowProcessFunction<?, W> windowFunction)
            throws Exception {
        Collection<W> windows =
                windowFunction.assignActualWindows(element, getUtcTimestamp(element, clock));
        checkState(windows.size() <= 1);
        if (windows.size() == 1) {
            return Optional.of(windows.iterator().next());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<W> assignStateNamespace(
            RowData element, ClockService clock, MergingWindowProcessFunction<?, W> windowFunction)
            throws Exception {
        Collection<W> windows =
                windowFunction.assignStateNamespace(element, getUtcTimestamp(element, clock));
        checkState(windows.size() <= 1);
        if (windows.size() == 1) {
            return Optional.of(windows.iterator().next());
        } else {
            return Optional.empty();
        }
    }

    protected long getUtcTimestamp(RowData element, ClockService clock) {
        final long timestamp;
        if (rowtimeIndex >= 0) {
            if (element.isNullAt(rowtimeIndex)) {
                throw new RuntimeException(
                        "RowTime field should not be null,"
                                + " please convert it to a non-null long value.");
            }
            // Precision for row timestamp is always 3
            TimestampData rowTime = element.getTimestamp(rowtimeIndex, 3);
            timestamp = toUtcTimestampMills(rowTime.getMillisecond(), shiftTimeZone);
        } else {
            // in processing time mode
            timestamp = toUtcTimestampMills(clock.currentProcessingTime(), shiftTimeZone);
        }
        return timestamp;
    }

    @Override
    public boolean isEventTime() {
        return isEventTime;
    }
}
