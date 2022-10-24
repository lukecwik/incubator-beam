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
package org.apache.beam.fn.harness.jmh;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.FusedPipeline;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.joda.time.DateTimeUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/** Benchmarks for processing a bundle end to end. */
public class ProcessBundleHandlerBenchmark {
  /** Sets up the {@link ExecutionStateTracker} and an execution state. */
  @State(Scope.Benchmark)
  public static class ProcessBundleHandlerHolder {
    final IdGenerator idGenerator;
    final ExecutionStateSampler executionStateSampler;
    final ConcurrentHashMap<String, ProcessBundleDescriptor> registry;
    final ConcurrentHashMap<String, Future<?>> dataFutures;
    final ProcessBundleHandler processBundleHandler;
    final List<Elements> elementsToSend;

    public ProcessBundleHandlerHolder() {
      idGenerator = IdGenerators.incrementingLongs();
      elementsToSend = new ArrayList<>();
      dataFutures = new ConcurrentHashMap<>();
      PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
      ExecutorService executorService =
          pipelineOptions.as(ExecutorOptions.class).getScheduledExecutorService();
      executionStateSampler =
          new ExecutionStateSampler(pipelineOptions, DateTimeUtils.SYSTEM_MILLIS_PROVIDER);
      registry = new ConcurrentHashMap<>();
      processBundleHandler =
          new ProcessBundleHandler(
              pipelineOptions,
              Collections.emptySet(),
              registry::get,
              new BeamFnDataClient() {
                @Override
                public void registerReceiver(
                    String instructionId,
                    List<ApiServiceDescriptor> apiServiceDescriptors,
                    CloseableFnDataReceiver<Elements> receiver) {
                  dataFutures.put(
                      instructionId,
                      executorService.submit(
                          () -> {
                            for (int i = 0, size = elementsToSend.size(); i < size; ++i) {
                              receiver.accept(elementsToSend.get(i));
                            }
                            //                receiver.close();
                            return null;
                          }));
                }

                @Override
                public void unregisterReceiver(
                    String instructionId, List<ApiServiceDescriptor> apiServiceDescriptors) {
                  try {
                    dataFutures.remove(instructionId).get();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }

                @Override
                public BeamFnDataOutboundAggregator createOutboundAggregator(
                    ApiServiceDescriptor apiServiceDescriptor,
                    Supplier<String> processBundleRequestIdSupplier,
                    boolean collectElementsIfNoFlushes) {
                  return new BeamFnDataOutboundAggregator(
                      pipelineOptions,
                      processBundleRequestIdSupplier,
                      TestStreams.<Elements>withOnNext(
                              (elements) -> {
                                assertEquals(2, elements.getDataCount());
                              })
                          .build(),
                      collectElementsIfNoFlushes);
                }
              },
              new BeamFnStateGrpcClientCache(
                  idGenerator,
                  ManagedChannelFactory.createDefault(),
                  OutboundObserverFactory.clientDirect()),
              new FinalizeBundleHandler(executorService),
              new ShortIdMap(),
              executionStateSampler,
              Caches.fromOptions(pipelineOptions));
    }

    @TearDown
    public void tearDown() throws Exception {
      processBundleHandler.shutdown();
    }
  }

  public abstract static class TrivialTransform extends ProcessBundleHandlerHolder {
    final ExecutableProcessBundleDescriptor descriptor;

    private static class OutputZeroOneTwo extends DoFn<byte[], String> {
      @ProcessElement
      public void process(ProcessContext ctxt) {
        ctxt.output("zero");
        ctxt.output("one");
        ctxt.output("two");
      }
    }

    private static class ConvertLength extends DoFn<String, Long> {
      @ProcessElement
      public void process(ProcessContext ctxt) {
        ctxt.output((long) ctxt.element().length());
      }
    }

    public TrivialTransform(int numElements) {
      try {
        Pipeline p = Pipeline.create();
        p.apply("impulse", Impulse.create())
            .apply("create", ParDo.of(new TrivialTransform.OutputZeroOneTwo()))
            .apply("len", ParDo.of(new TrivialTransform.ConvertLength()))
            .apply("addKeys", WithKeys.of("foo"))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()))
            // Force the output to be materialized
            .apply("gbk", GroupByKey.create());

        RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
        // The fuser will create one stage (SDK responsible portion in []):
        // (Impulse + [create + len + addKeys] + GBK write)
        //
        // We use the one stage containing the DoFns and run a benchmark expecting the SDK
        // to accept byte[] and output KV<String, Long>.
        FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
        checkState(fused.getFusedStages().size() == 1, "Expected exactly one fused stage");
        ExecutableStage stage = fused.getFusedStages().iterator().next();

        this.descriptor =
            ProcessBundleDescriptors.fromExecutableStage(
                "my_stage", stage, ApiServiceDescriptor.getDefaultInstance());

        this.registry.put("descriptorId", descriptor.getProcessBundleDescriptor());

        ByteStringOutputStream elementsData = new ByteStringOutputStream();
        for (int i = 0; i < numElements; ++i) {
          descriptor
              .getRemoteInputDestinations()
              .get(0)
              .getCoder()
              .encode(valueInGlobalWindow(new byte[0]), elementsData);
        }
        elementsToSend.add(
            Elements.newBuilder()
                .addData(
                    Elements.Data.newBuilder()
                        .setTransformId(
                            descriptor.getRemoteInputDestinations().get(0).getPTransformId())
                        .setData(elementsData.toByteString())
                        .build())
                .build());
        elementsToSend.add(
            Elements.newBuilder()
                .addData(
                    Elements.Data.newBuilder()
                        .setTransformId(
                            descriptor.getRemoteInputDestinations().get(0).getPTransformId())
                        .setIsLast(true)
                        .build())
                .build());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @State(Scope.Benchmark)
  public static class TinyBundleTrivialTransform extends TrivialTransform {
    public TinyBundleTrivialTransform() {
      super(3);
    }
  }

  @State(Scope.Benchmark)
  public static class LargeBundleTrivialTransform extends TrivialTransform {
    public LargeBundleTrivialTransform() {
      super(1000);
    }
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during bundle processing.
  public void testTinyBundle(TinyBundleTrivialTransform trivialTransform) throws Exception {
    InstructionRequest request =
        InstructionRequest.newBuilder()
            .setProcessBundle(
                ProcessBundleRequest.newBuilder()
                    .setProcessBundleDescriptorId("descriptorId")
                    .build())
            .setInstructionId("instructionId" + trivialTransform.idGenerator.getId())
            .build();

    InstructionResponse.Builder response =
        trivialTransform.processBundleHandler.processBundle(request);
    assertTrue(response.hasProcessBundle());
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during bundle processing.
  public void testLargeBundle(LargeBundleTrivialTransform trivialTransform) throws Exception {
    InstructionRequest request =
        InstructionRequest.newBuilder()
            .setProcessBundle(
                ProcessBundleRequest.newBuilder()
                    .setProcessBundleDescriptorId("descriptorId")
                    .build())
            .setInstructionId("instructionId" + trivialTransform.idGenerator.getId())
            .build();

    InstructionResponse.Builder response =
        trivialTransform.processBundleHandler.processBundle(request);
    assertTrue(response.hasProcessBundle());
  }
}
