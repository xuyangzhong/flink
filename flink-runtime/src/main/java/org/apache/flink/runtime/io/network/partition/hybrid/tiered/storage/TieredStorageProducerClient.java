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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Client of the Tiered Storage used by the producer. */
public class TieredStorageProducerClient {

    private final boolean isBroadcastOnly;

    private final int numSubpartitions;

    private final BufferAccumulator bufferAccumulator;

    private final BufferCompressor bufferCompressor;

    /**
     * Note that the {@link TierProducerAgent}s are sorted by priority, with a lower index
     * indicating a higher priority.
     */
    private final List<TierProducerAgent> tierProducerAgents;

    /** The current writing segment index for each subpartition. */
    private final int[] currentSubpartitionSegmentId;

    /** The current writing storage tier for each subpartition. */
    private final TierProducerAgent[] currentSubpartitionTierAgent;

    /**
     * The metric statistics for producer client. Note that it is necessary to check whether the
     * value is null before used.
     */
    @Nullable private Consumer<TieredStorageProducerMetricUpdate> metricStatisticsUpdater;

    public TieredStorageProducerClient(
            int numSubpartitions,
            boolean isBroadcastOnly,
            BufferAccumulator bufferAccumulator,
            @Nullable BufferCompressor bufferCompressor,
            List<TierProducerAgent> tierProducerAgents) {
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitions = numSubpartitions;
        this.bufferAccumulator = bufferAccumulator;
        this.bufferCompressor = bufferCompressor;
        this.tierProducerAgents = tierProducerAgents;
        this.currentSubpartitionSegmentId = new int[numSubpartitions];
        this.currentSubpartitionTierAgent = new TierProducerAgent[numSubpartitions];

        Arrays.fill(currentSubpartitionSegmentId, -1);

        bufferAccumulator.setup(this::writeAccumulatedBuffer);
    }

    /**
     * Write records to the producer client. The {@link BufferAccumulator} will accumulate the
     * records into buffers.
     *
     * <p>Note that isBroadcast indicates whether the record is broadcast, while isBroadcastOnly
     * indicates whether the result partition is broadcast-only. When the result partition is not
     * broadcast-only and the record is a broadcast record, the record will be written to all the
     * subpartitions.
     *
     * @param record the written record data
     * @param subpartitionId the subpartition identifier
     * @param dataType the data type of the record
     * @param isBroadcast whether the record is a broadcast record
     */
    public void write(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {

        if (isBroadcast && !isBroadcastOnly) {
            int currentPosition = record.position();
            for (int i = 0; i < numSubpartitions; ++i) {
                // As the tiered storage subpartition ID is created only for broadcast records,
                // which are fewer than normal records, the performance impact of generating new
                // TieredStorageSubpartitionId objects is expected to be manageable. If the
                // performance is significantly affected, this logic will be optimized accordingly.
                bufferAccumulator.receive(
                        record, new TieredStorageSubpartitionId(i), dataType, isBroadcast);
                record.position(currentPosition);
            }
        } else {
            bufferAccumulator.receive(record, subpartitionId, dataType, isBroadcast);
        }
    }

    public void setMetricStatisticsUpdater(
            Consumer<TieredStorageProducerMetricUpdate> metricStatisticsUpdater) {
        this.metricStatisticsUpdater = checkNotNull(metricStatisticsUpdater);
    }

    public void close() {
        bufferAccumulator.close();
        tierProducerAgents.forEach(TierProducerAgent::close);
    }

    /**
     * Write the accumulated buffer of this subpartitionId to an appropriate tier. After the tier is
     * decided, the buffer will be written to the selected tier.
     *
     * <p>Note that the method only throws an exception when choosing a storage tier, so the caller
     * should ensure that the buffer is recycled when throwing an exception.
     *
     * @param subpartitionId the subpartition identifier
     * @param accumulatedBuffer one accumulated buffer of this subpartition
     */
    private void writeAccumulatedBuffer(
            TieredStorageSubpartitionId subpartitionId,
            Buffer accumulatedBuffer,
            int numRemainingConsecutiveBuffers) {
        int unCompressedSize = accumulatedBuffer.readableBytes();
        try {
            if (currentSubpartitionTierAgent[subpartitionId.getSubpartitionId()] == null) {
                chooseStorageTierToStartSegment(subpartitionId, numRemainingConsecutiveBuffers + 1);
            }
            if (!currentSubpartitionTierAgent[subpartitionId.getSubpartitionId()].tryWrite(
                    subpartitionId,
                    accumulatedBuffer,
                    bufferAccumulator,
                    numRemainingConsecutiveBuffers)) {
                chooseStorageTierToStartSegment(subpartitionId, numRemainingConsecutiveBuffers + 1);
                checkState(
                        currentSubpartitionTierAgent[subpartitionId.getSubpartitionId()].tryWrite(
                                subpartitionId,
                                accumulatedBuffer,
                                bufferAccumulator,
                                numRemainingConsecutiveBuffers),
                        "Failed to write the first buffer to the new segment");
            }
        } catch (IOException ioe) {
            accumulatedBuffer.recycleBuffer();
            ExceptionUtils.rethrow(ioe);
        }
        updateMetricStatistics(1, unCompressedSize);
    }

    private void chooseStorageTierToStartSegment(
            TieredStorageSubpartitionId subpartitionId, int totalNumBuffers) throws IOException {
        int subpartitionIndex = subpartitionId.getSubpartitionId();
        int segmentIndex = currentSubpartitionSegmentId[subpartitionIndex];
        int nextSegmentIndex = segmentIndex + 1;

        for (TierProducerAgent tierProducerAgent : tierProducerAgents) {
            if (tierProducerAgent.tryStartNewSegment(
                    subpartitionId, nextSegmentIndex, totalNumBuffers)) {
                // Update the segment index and the chosen storage tier for the subpartition.
                currentSubpartitionSegmentId[subpartitionIndex] = nextSegmentIndex;
                currentSubpartitionTierAgent[subpartitionIndex] = tierProducerAgent;
                return;
            }
        }
        throw new IOException("Failed to choose a storage tier to start a new segment.");
    }

    private void updateMetricStatistics(int numWriteBuffersDelta, int numWriteBytesDelta) {
        checkNotNull(metricStatisticsUpdater)
                .accept(
                        new TieredStorageProducerMetricUpdate(
                                numWriteBuffersDelta, numWriteBytesDelta));
    }
}
