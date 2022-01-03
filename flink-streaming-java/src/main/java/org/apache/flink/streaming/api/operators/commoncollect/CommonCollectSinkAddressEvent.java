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

package org.apache.flink.streaming.api.operators.commoncollect;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.net.InetSocketAddress;

/**
 * An {@link OperatorEvent} that passes the operator id and socket server address in the sink to the
 * coordinator.
 */
public class CommonCollectSinkAddressEvent implements OperatorEvent {
    private static final long serialVersionUID = 1L;

    private final String operatorId;

    private final InetSocketAddress address;

    public CommonCollectSinkAddressEvent(String operatorId, InetSocketAddress address) {
        this.operatorId = operatorId;
        this.address = address;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public String getOperatorId() {
        return operatorId;
    }
}
