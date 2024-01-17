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
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.SessionWindowAssigner;

import java.time.Duration;
import java.time.ZoneId;

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

        private final SessionWindowAssigner innerSessionWindowAssigner;

        public SessionUnsliceAssigner(int rowtimeIndex, ZoneId shiftTimeZone, long sessionGap) {
            super(rowtimeIndex, shiftTimeZone);
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
    }
}
