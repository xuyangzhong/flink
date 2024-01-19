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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.MergeCallback;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.InternalTimeWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.SessionWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.internal.MergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.windowtvf.common.ClockService;
import org.apache.flink.table.runtime.operators.window.windowtvf.slicing.SliceAssigner;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.NavigableSet;
import java.util.Optional;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;

/** Utilities to create {@link UnsliceAssigner}s. */
@Internal
public class UnsliceAssigners {

    /**
     * Creates a session window {@link UnsliceAssigner} that assigns elements to windows based on
     * the timestamp.
     *
     * @param rowtimeIndex The index of rowtime field in the input row, {@code -1} if based on
     *     processing time.
     * @param shiftTimeZone The shift timezone of the window, if the proctime or rowtime type is
     *     TIMESTAMP_LTZ, the shift timezone is the timezone user configured in TableConfig, other
     *     cases the timezone is UTC which means never shift when assigning windows.
     * @param gap The session timeout, i.e. the time gap between sessions
     */
    public static SessionUnsliceAssigner session(
            int rowtimeIndex, ZoneId shiftTimeZone, Duration gap) {
        return new SessionUnsliceAssigner(rowtimeIndex, shiftTimeZone, gap.toMillis());
    }

    /** The {@link UnsliceAssigner} for session windows. */
    public static class SessionUnsliceAssigner extends AbstractUnsliceAssigner<TimeWindow> {

        private static final long serialVersionUID = 1L;

        private final long sessionGap;

        private final SessionWindowAssigner innerSessionWindowAssigner;

        public SessionUnsliceAssigner(int rowtimeIndex, ZoneId shiftTimeZone, long sessionGap) {
            super(rowtimeIndex, shiftTimeZone);
            this.sessionGap = sessionGap;
            this.innerSessionWindowAssigner =
                    SessionWindowAssigner.withGap(Duration.ofMillis(sessionGap));
            if (isEventTime()) {
                this.innerSessionWindowAssigner.withEventTime();
            } else {
                this.innerSessionWindowAssigner.withProcessingTime();
            }
        }

        @Override
        public MergingWindowAssigner<TimeWindow> getInnerMergingWindowAssigner() {
            return innerSessionWindowAssigner;
        }

        @Override
        public String getDescription() {
            return String.format("SessionWindow(gap=%dms)", sessionGap);
        }
    }

    /**
     * Creates a {@link SliceAssigner} that assigns elements which has been attached window start
     * and window end timestamp to slices. The assigned slice is equal to the given window.
     *
     * @param windowStartIndex the index of window start field in the input row, mustn't be a
     *     negative value.
     * @param windowEndIndex the index of window end field in the input row, mustn't be a negative
     *     value.
     * @param shiftTimeZone The shift timezone of the window, if the proctime or rowtime type is
     *     TIMESTAMP_LTZ, the shift timezone is the timezone user configured in TableConfig, other
     *     cases the timezone is UTC which means never shift when assigning windows.
     */
    public static WindowedUnsliceAssigner windowed(
            int windowStartIndex,
            int windowEndIndex,
            UnsliceAssigner<TimeWindow> innerAssigner,
            ZoneId shiftTimeZone) {
        return new WindowedUnsliceAssigner(
                windowStartIndex, windowEndIndex, innerAssigner, shiftTimeZone);
    }

    /**
     * The {@link UnsliceAssigner} for elements have been merged into unslicing windows and attached
     * window start and end timestamps.
     */
    public static class WindowedUnsliceAssigner extends MergingWindowAssigner<TimeWindow>
            implements UnsliceAssigner<TimeWindow>, InternalTimeWindowAssigner {

        private static final long serialVersionUID = 1L;

        private final int windowStartIndex;

        private final int windowEndIndex;

        private final UnsliceAssigner<TimeWindow> innerAssigner;

        private final ZoneId shiftTimeZone;

        public WindowedUnsliceAssigner(
                int windowStartIndex,
                int windowEndIndex,
                UnsliceAssigner<TimeWindow> innerAssigner,
                ZoneId shiftTimeZone) {
            this.windowStartIndex = windowStartIndex;
            this.windowEndIndex = windowEndIndex;
            this.innerAssigner = innerAssigner;
            this.shiftTimeZone = shiftTimeZone;
        }

        @Override
        public Optional<TimeWindow> assignActualWindow(
                RowData element,
                ClockService clock,
                MergingWindowProcessFunction<?, TimeWindow> windowFunction)
                throws Exception {
            return Optional.of(createWindow(element));
        }

        @Override
        public Optional<TimeWindow> assignStateNamespace(
                RowData element,
                ClockService clock,
                MergingWindowProcessFunction<?, TimeWindow> windowFunction)
                throws Exception {
            return Optional.of(createWindow(element));
        }

        private TimeWindow createWindow(RowData element) {
            if (element.isNullAt(windowStartIndex) || element.isNullAt(windowEndIndex)) {
                throw new RuntimeException("RowTime field should not be null.");
            }
            // Precision for row timestamp is always 3
            final long windowStartTime =
                    toUtcTimestampMills(
                            element.getTimestamp(windowStartIndex, 3).getMillisecond(),
                            shiftTimeZone);
            final long windowEndTime =
                    toUtcTimestampMills(
                            element.getTimestamp(windowStartIndex, 3).getMillisecond(),
                            shiftTimeZone);
            return new TimeWindow(windowStartTime, windowEndTime);
        }

        @Override
        public MergingWindowAssigner<TimeWindow> getInnerMergingWindowAssigner() {
            return this;
        }

        @Override
        public boolean isEventTime() {
            // it always works in event-time mode if input row has been attached windows
            return true;
        }

        @Override
        public Collection<TimeWindow> assignWindows(RowData element, long timestamp)
                throws IOException {
            return Collections.singletonList(createWindow(element));
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public String toString() {
            return getDescription();
        }

        @Override
        public String getDescription() {
            return String.format(
                    "WindowedUnsliceAssigner(innerAssigner=%s, StartIndex=%d, windowEndIndex=%d)",
                    innerAssigner.getDescription(), windowStartIndex, windowEndIndex);
        }

        @Override
        public InternalTimeWindowAssigner withEventTime() {
            throw new IllegalStateException(
                    "Should not call this function on WindowedUnsliceAssigner.");
        }

        @Override
        public InternalTimeWindowAssigner withProcessingTime() {
            throw new IllegalStateException(
                    "Should not call this function on WindowedUnsliceAssigner.");
        }

        @Override
        public void mergeWindows(
                TimeWindow newWindow,
                NavigableSet<TimeWindow> sortedWindows,
                MergeCallback<TimeWindow, Collection<TimeWindow>> callback) {
            // no need to merge windows because the upstream has done it.
        }
    }
}
