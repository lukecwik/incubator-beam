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
package org.apache.beam.sdk.fn.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.DataStreams;
import org.apache.beam.sdk.fn.stream.DataStreams.ElementDelimitedOutputStream;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A buffering outbound {@link FnDataReceiver} for the Beam Fn Data API.
 *
 * <p>Encodes individually consumed elements with the provided {@link Coder} producing
 * a single {@link BeamFnApi.Elements} message when the buffer threshold
 * is surpassed.
 */
public class BeamFnDataBufferingOutboundObserver<T>
    implements CloseableFnDataReceiver<WindowedValue<T>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(BeamFnDataBufferingOutboundObserver.class);

  public static <T> BeamFnDataBufferingOutboundObserver<T> forLocation(
      LogicalEndpoint endpoint,
      Coder<WindowedValue<T>> coder,
      StreamObserver<BeamFnApi.Elements> outboundObserver) {
    return forLocationWithBufferLimit(
        DataStreams.DEFAULT_OUTBOUND_BUFFER_LIMIT_BYTES, endpoint, coder, outboundObserver);
  }

  public static <T> BeamFnDataBufferingOutboundObserver<T> forLocationWithBufferLimit(
      int bufferLimit,
      LogicalEndpoint endpoint,
      Coder<WindowedValue<T>> coder,
      StreamObserver<BeamFnApi.Elements> outboundObserver) {
    return new BeamFnDataBufferingOutboundObserver<>(
        bufferLimit, endpoint, coder, outboundObserver);
  }

  private long byteCounter;
  private long counter;
  private boolean closed;
  private final Coder<WindowedValue<T>> coder;
  private final LogicalEndpoint outputLocation;
  private final StreamObserver<BeamFnApi.Elements> outboundObserver;
  private final ElementDelimitedOutputStream outputStream;

  private BeamFnDataBufferingOutboundObserver(
      int bufferLimit,
      LogicalEndpoint outputLocation,
      Coder<WindowedValue<T>> coder,
      StreamObserver<BeamFnApi.Elements> outboundObserver) {
    this.outputLocation = outputLocation;
    this.coder = coder;
    this.outboundObserver = outboundObserver;
    this.outputStream = DataStreams.outbound(this::transmitChunk, bufferLimit);
    this.closed = false;
  }

  private void transmitChunk(ByteString bytes) {
    byteCounter += bytes.size();
    BeamFnApi.Elements.Builder elements = BeamFnApi.Elements.newBuilder();
    elements.addDataBuilder()
        .setInstructionReference(outputLocation.getInstructionId())
        .setTarget(outputLocation.getTarget())
        .setData(bytes);
    outboundObserver.onNext(elements.build());
  }

  @Override
  public void close() throws Exception {
    if (closed) {
      throw new IllegalStateException("Already closed.");
    }
    closed = true;
    outputStream.close();

    // This will add an empty data block representing the end of stream.
    BeamFnApi.Elements.Builder elements = BeamFnApi.Elements.newBuilder();
    elements.addDataBuilder()
        .setInstructionReference(outputLocation.getInstructionId())
        .setTarget(outputLocation.getTarget());

    LOG.debug("Closing stream for instruction {} and "
        + "target {} having transmitted {} values {} bytes",
        outputLocation.getInstructionId(),
        outputLocation.getTarget(),
        counter,
        byteCounter);
    outboundObserver.onNext(elements.build());
  }

  @Override
  public void accept(WindowedValue<T> t) throws IOException {
    if (closed) {
      throw new IllegalStateException("Already closed.");
    }
    counter += 1;
    coder.encode(t, outputStream);
    outputStream.delimitElement();
  }
}
