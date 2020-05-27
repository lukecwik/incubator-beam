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
package org.apache.beam.runners.dataflow.worker;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Materialization;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * The minimal amount of information required to create and use a {@link SideInputReader} when using
 * the portability framework within the Dataflow Runner harness.
 *
 * <p>Note that this is being used to satisfy type constraints for {@link SideInputReader} used
 * throughout the Dataflow Runner harness and only supports returning the tag, the coder, the
 * materialization, and the window coder. All other methods throw {@link
 * UnsupportedOperationException}.
 *
 * <p>TODO: Migrate to a runner only specific concept of a side input to be used with {@link
 * SideInputReader}s.
 */
public class DataflowPortabilityPCollectionView<ViewT, W extends BoundedWindow>
    implements PCollectionView<ViewT> {

  public static <ViewT> PCollectionView<ViewT> with(
      TupleTag<?> tag, FullWindowedValueCoder<?> coder, Materialization<ViewT> materialization) {
    return new DataflowPortabilityPCollectionView(tag, coder, materialization);
  }

  private final TupleTag<?> tag;
  private final FullWindowedValueCoder<?> coder;
  private final ViewFn<?, ViewT> viewFn;

  private DataflowPortabilityPCollectionView(
      TupleTag<?> tag, FullWindowedValueCoder<?> coder, Materialization<ViewT> materialization) {
    this.tag = tag;
    this.coder = coder;
    this.viewFn = new PortabilityViewFn<>(materialization);
  }

  @Nullable
  @Override
  public PCollection<?> getPCollection() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TupleTag<?> getTagInternal() {
    return tag;
  }

  @Override
  public ViewFn<?, ViewT> getViewFn() {
    return viewFn;
  }

  /**
   * A minimal type {@link ViewFn} that satisfies requirements to be used when executing portable
   * pipelines.
   */
  public static class PortabilityViewFn<ViewT> extends ViewFn<ViewT, ViewT> {
    private final Materialization<ViewT> materialization;

    // prevent instantiation outside of class
    private PortabilityViewFn(Materialization<ViewT> materialization) {
      this.materialization = materialization;
    }

    @Override
    public Materialization<ViewT> getMaterialization() {
      return materialization;
    }

    @Override
    public ViewT apply(ViewT o) {
      return o;
    }

    @Override
    public TypeDescriptor<ViewT> getTypeDescriptor() {
      throw new UnsupportedOperationException();
    }
  };

  @Override
  public WindowMappingFn<?> getWindowMappingFn() {
    throw new UnsupportedOperationException();
  }

  @Override
  public WindowingStrategy<?, W> getWindowingStrategyInternal() {
    return WindowingStrategy.of(
        new WindowFn<Object, W>() {
          @Override
          public Collection<W> assignWindows(AssignContext c) throws Exception {
            throw new UnsupportedOperationException();
          }

          @Override
          public void mergeWindows(MergeContext c) throws Exception {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isCompatible(WindowFn<?, ?> other) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Coder<W> windowCoder() {
            return (Coder) coder.getWindowCoder();
          }

          @Override
          public WindowMappingFn<W> getDefaultWindowMappingFn() {
            throw new UnsupportedOperationException();
          }
        });
  }

  @Override
  public Coder<?> getCoderInternal() {
    return coder.getValueCoder();
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Pipeline getPipeline() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {
    throw new UnsupportedOperationException();
  }
}
