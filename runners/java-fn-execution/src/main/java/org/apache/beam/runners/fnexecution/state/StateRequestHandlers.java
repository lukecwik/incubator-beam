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

package org.apache.beam.runners.fnexecution.state;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.BagUserStateSpec;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.MultimapSideInputSpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.fn.stream.DataStreams;
import org.apache.beam.sdk.fn.stream.DataStreams.ElementDelimitedOutputStream;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.common.Reiterable;

/**
 * A set of utility methods which construct {@link StateRequestHandler}s.
 *
 * <p>TODO: Add a variant which works on {@link ByteString}s to remove encoding/decoding overhead.
 */
public class StateRequestHandlers {

  /**
   * A handler for multimap side inputs.
   *
   * <p>Note that this handler is expected to be thread safe as it will be invoked concurrently.
   */
  @ThreadSafe
  public interface MultimapSideInputHandler<K, V, W extends BoundedWindow> {
    /**
     * Returns an {@link Iterable} of values representing the side input for the given key and
     * window.
     *
     * <p>TODO: Add support for side input chunking and caching if a {@link Reiterable} is returned.
     */
    Iterable<V> get(K key, W window);
  }

  /**
   * A factory which constructs {@link MultimapSideInputHandler}s.
   *
   * <p>Note that this factory should be thread safe because it will be invoked concurrently.
   */
  @ThreadSafe
  public interface MultimapSideInputHandlerFactory {

    /**
     * Returns a {@link MultimapSideInputHandler} for the given {@code pTransformId} and
     * {@code sideInputId}. The supplied {@code keyCoder}, {@code valueCoder}, and
     * {@code windowCoder} should be used to encode/decode their respective values.
     */
    <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> forSideInput(
        String pTransformId,
        String sideInputId,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder);

    /**
     * Throws a {@link UnsupportedOperationException} on the first access.
     */
    static MultimapSideInputHandlerFactory unsupported() {
      return new MultimapSideInputHandlerFactory() {
        @Override
        public <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> forSideInput(
            String pTransformId, String sideInputId, Coder<K> keyCoder, Coder<V> valueCoder,
            Coder<W> windowCoder) {
          throw new UnsupportedOperationException(String.format(
              "The %s does not support handling sides inputs for PTransform %s with side "
                  + "input id %s.",
              MultimapSideInputHandler.class.getSimpleName(),
              pTransformId,
              sideInputId));
        }
      };
    }
  }

  /**
   * A handler for bag user state.
   *
   * <p>Note that this handler is expected to be thread safe as it will be invoked concurrently.
   */
  @ThreadSafe
  public interface BagUserStateHandler<K, V, W extends BoundedWindow> {
    /**
     * Returns an {@link Iterable} of values representing the bag user state for the given key and
     * window.
     *
     * <p>TODO: Add support for bag user state chunking and caching if a {@link Reiterable} is
     * returned.
     */
    Iterable<V> get(K key, W window);

    /**
     * Appends the values to the bag user state for the given key and window.
     */
    void append(K key, W window, Iterator<V> values);

    /**
     * Clears the bag user state for the given key and window.
     */
    void clear(K key, W window);
  }

  /**
   * A factory which constructs {@link BagUserStateHandler}s.
   *
   * <p>Note that this factory should be thread safe.
   */
  @ThreadSafe
  public interface BagUserStateHandlerFactory {
    <K, V, W extends BoundedWindow> BagUserStateHandler<K, V, W> forUserState(
        String pTransformId,
        String userStateId,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder);

    /**
     * Throws a {@link UnsupportedOperationException} on the first access.
     */
    static BagUserStateHandlerFactory unsupported() {
      return new BagUserStateHandlerFactory() {
        @Override
        public <K, V, W extends BoundedWindow> BagUserStateHandler<K, V, W> forUserState(
            String pTransformId, String userStateId, Coder<K> keyCoder, Coder<V> valueCoder,
            Coder<W> windowCoder) {
          throw new UnsupportedOperationException(String.format(
              "The %s does not support handling sides inputs for PTransform %s with user state "
                  + "id %s.",
              BagUserStateHandler.class.getSimpleName(),
              pTransformId,
              userStateId));
        }
      };
    }
  }

  /**
   * Returns a {@link StateRequestHandler} which delegates to the supplied handler depending
   * on the {@link StateRequest}s {@link StateKey.TypeCase type}.
   *
   * <p>An exception is thrown if a corresponding handler is not found.
   */
  public static StateRequestHandler delegateBasedUponType(
      EnumMap<StateKey.TypeCase, StateRequestHandler> handlers) {
    return new StateKeyTypeDelegatingStateRequestHandler(handlers);
  }

  /**
   * A {@link StateRequestHandler} which delegates to the supplied handler depending
   * on the {@link StateRequest}s {@link StateKey.TypeCase type}.
   *
   * <p>An exception is thrown if a corresponding handler is not found.
   */
  static class StateKeyTypeDelegatingStateRequestHandler implements StateRequestHandler {
    private final EnumMap<TypeCase, StateRequestHandler> handlers;

    StateKeyTypeDelegatingStateRequestHandler(
        EnumMap<StateKey.TypeCase, StateRequestHandler> handlers) {
      this.handlers = handlers;
    }

    @Override
    public CompletionStage<StateResponse.Builder> handle(StateRequest request) throws Exception {
      return handlers.getOrDefault(request.getStateKey().getTypeCase(), this::handlerNotFound)
          .handle(request);
    }

    private CompletionStage<StateResponse.Builder> handlerNotFound(StateRequest request) {
      CompletableFuture<StateResponse.Builder> rval = new CompletableFuture<>();
      rval.completeExceptionally(new IllegalStateException());
      return rval;
    }
  }

  /**
   * Returns an adapter which converts a {@link MultimapSideInputHandlerFactory} to a
   * {@link StateRequestHandler}.
   *
   * <p>The {@link MultimapSideInputHandlerFactory} is required to handle all multimap side inputs
   * contained within the {@link ExecutableProcessBundleDescriptor}. See
   * {@link ExecutableProcessBundleDescriptor#getMultimapSideInputSpecs} for the set of multimap
   * side inputs that are contained.
   *
   * <p>Instances of {@link MultimapSideInputHandler}s returned by the
   * {@link MultimapSideInputHandlerFactory} are cached.
   */
  public static StateRequestHandler forMultimapSideInputHandlerFactory(
      ExecutableProcessBundleDescriptor processBundleDescriptor,
      MultimapSideInputHandlerFactory multimapSideInputHandlerFactory) {
    return new StateRequestHandlerToMultimapSideInputHandlerFactoryAdapter(
        processBundleDescriptor, multimapSideInputHandlerFactory);
  }

  /**
   * An adapter which converts {@link MultimapSideInputHandlerFactory} to
   * {@link StateRequestHandler}.
   */
  static class StateRequestHandlerToMultimapSideInputHandlerFactoryAdapter
      implements StateRequestHandler {

    private final ExecutableProcessBundleDescriptor processBundleDescriptor;
    private final MultimapSideInputHandlerFactory handlerFactory;
    private final ConcurrentHashMap<MultimapSideInputSpec, MultimapSideInputHandler> cache;

    StateRequestHandlerToMultimapSideInputHandlerFactoryAdapter(
        ExecutableProcessBundleDescriptor processBundleDescriptor,
        MultimapSideInputHandlerFactory handlerFactory) {
      this.processBundleDescriptor = processBundleDescriptor;
      this.handlerFactory = handlerFactory;
      this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public CompletionStage<StateResponse.Builder> handle(
        StateRequest request) throws Exception {
      try {
        checkState(TypeCase.MULTIMAP_SIDE_INPUT.equals(request.getStateKey().getTypeCase()),
            "Unsupported %s type %s, expected %s",
            StateRequest.class.getSimpleName(),
            request.getStateKey().getTypeCase(),
            TypeCase.MULTIMAP_SIDE_INPUT);

        StateKey.MultimapSideInput stateKey = request.getStateKey().getMultimapSideInput();
        MultimapSideInputSpec<Object, Object, BoundedWindow> referenceSpec =
            processBundleDescriptor.getMultimapSideInputSpecs()
                .get(stateKey.getPtransformId())
                .get(stateKey.getSideInputId());
        MultimapSideInputHandler<Object, Object, BoundedWindow> handler = cache.computeIfAbsent(
            referenceSpec,
            this::createHandler);

        // TODO: Migrate to using the ByteStringCoder everywhere for user types and values
        // to reduce the amount of byte array copying.
        Object key = referenceSpec.keyCoder().decode(stateKey.getKey().newInput());
        BoundedWindow window = referenceSpec.windowCoder().decode(stateKey.getWindow().newInput());

        switch (request.getRequestCase()) {
          case GET:
            return handleGetRequest(request, key, window, referenceSpec.valueCoder(), handler);
          case APPEND:
          case CLEAR:
          default:
            throw new Exception(String.format(
                "Unsupported request type %s for side input.", request.getRequestCase()));
        }
      } catch (Exception e) {
        CompletableFuture f = new CompletableFuture();
        f.completeExceptionally(e);
        return f;
      }
    }

    private static <K, V, W extends BoundedWindow> CompletionStage<StateResponse.Builder>
    handleGetRequest(
        StateRequest request,
        K key,
        W window,
        Coder<V> valueCoder,
        MultimapSideInputHandler<K, V, W> handler) throws Exception {
      // TODO: Add support for continuation tokens when handling state if the handler
      // returned a {@link Reiterable}.
      checkState(request.getGet().getContinuationToken().isEmpty(),
          "Continuation tokens are unsupported.");

      Iterable<V> values = handler.get(key, window);
      List<ByteString> encodedValues = new ArrayList<>();
      ElementDelimitedOutputStream outputStream = DataStreams.outbound(encodedValues::add);
      for (V value : values) {
        valueCoder.encode(value, outputStream);
        outputStream.delimitElement();
      }
      outputStream.close();

      StateResponse.Builder response = StateResponse.newBuilder();
      response.setId(request.getId());
      response.setGet(
          StateGetResponse.newBuilder()
              .setData(ByteString.copyFrom(encodedValues))
              .build());
      return CompletableFuture.completedFuture(response);
    }

    private <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> createHandler(
        MultimapSideInputSpec cacheKey) {
      return handlerFactory.forSideInput(
          cacheKey.transformId(),
          cacheKey.sideInputId(),
          cacheKey.keyCoder(),
          cacheKey.valueCoder(),
          cacheKey.windowCoder());
    }
  }

  /**
   * Returns an adapter which converts a {@link BagUserStateHandlerFactory} to a
   * {@link StateRequestHandler}.
   *
   * <p>The {@link MultimapSideInputHandlerFactory} is required to handle all multimap side inputs
   * contained within the {@link ExecutableProcessBundleDescriptor}. See
   * {@link ExecutableProcessBundleDescriptor#getMultimapSideInputSpecs} for the set of multimap
   * side inputs that are contained.
   *
   * <p>Instances of {@link MultimapSideInputHandler}s returned by the
   * {@link MultimapSideInputHandlerFactory} are cached.
   */
  public static StateRequestHandler forBagUserStateHandlerFactory(
      ExecutableProcessBundleDescriptor processBundleDescriptor,
      BagUserStateHandlerFactory bagUserStateHandlerFactory) {
    return new ByteStringStateRequestHandlerToBagUserStateHandlerFactoryAdapter(
        processBundleDescriptor, bagUserStateHandlerFactory);
  }

  /**
   * An adapter which converts {@link BagUserStateHandlerFactory} to
   * {@link StateRequestHandler}.
   */
  static class ByteStringStateRequestHandlerToBagUserStateHandlerFactoryAdapter
      implements StateRequestHandler {

    private final ExecutableProcessBundleDescriptor processBundleDescriptor;
    private final BagUserStateHandlerFactory handlerFactory;
    private final ConcurrentHashMap<BagUserStateSpec, BagUserStateHandler> cache;

    ByteStringStateRequestHandlerToBagUserStateHandlerFactoryAdapter(
        ExecutableProcessBundleDescriptor processBundleDescriptor,
        BagUserStateHandlerFactory handlerFactory) {
      this.processBundleDescriptor = processBundleDescriptor;
      this.handlerFactory = handlerFactory;
      this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public CompletionStage<StateResponse.Builder> handle(
        StateRequest request) throws Exception {
      try {
        checkState(TypeCase.BAG_USER_STATE.equals(request.getStateKey().getTypeCase()),
            "Unsupported %s type %s, expected %s",
            StateRequest.class.getSimpleName(),
            request.getStateKey().getTypeCase(),
            TypeCase.BAG_USER_STATE);

        StateKey.BagUserState stateKey = request.getStateKey().getBagUserState();
        BagUserStateSpec<Object, Object, BoundedWindow> referenceSpec =
            processBundleDescriptor.getBagUserStateSpecs()
                .get(stateKey.getPtransformId())
                .get(stateKey.getUserStateId());

        // Note that by using the ByteStringCoder, we simplify the issue of encoding/decoding the
        // logical stream because we do not need to maintain knowledge of element boundaries and
        // instead we rely on the client to be internally consistent. This allows us to just
        // take the append requests and also to serve them back without internal knowledge.
        checkState(
            ((Coder) referenceSpec.keyCoder()) instanceof ByteStringCoder,
            "This %s only supports the %s as the key coder.",
            BagUserStateHandlerFactory.class.getSimpleName(),
            ByteStringCoder.class.getSimpleName());
        checkState(
            ((Coder) referenceSpec.valueCoder()) instanceof ByteStringCoder,
            "This %s only supports the %s as the value coder.",
            BagUserStateHandlerFactory.class.getSimpleName(),
            ByteStringCoder.class.getSimpleName());

        BagUserStateHandler<ByteString, ByteString, BoundedWindow> handler = cache.computeIfAbsent(
            referenceSpec,
            this::createHandler);

        ByteString key = stateKey.getKey();
        BoundedWindow window = referenceSpec.windowCoder().decode(stateKey.getWindow().newInput());

        switch (request.getRequestCase()) {
          case GET:
            return handleGetRequest(request, key, window, handler);
          case APPEND:
            return handleAppendRequest(request, key, window, handler);
          case CLEAR:
            return handleClearRequest(request, key, window, handler);
          default:
            throw new Exception(String.format(
                "Unsupported request type %s for user state.", request.getRequestCase()));
        }
      } catch (Exception e) {
        CompletableFuture f = new CompletableFuture();
        f.completeExceptionally(e);
        return f;
      }
    }

    private static <W extends BoundedWindow> CompletionStage<StateResponse.Builder>
    handleGetRequest(
        StateRequest request,
        ByteString key,
        W window,
        BagUserStateHandler<ByteString, ByteString, W> handler) {
      // TODO: Add support for continuation tokens when handling state if the handler
      // returned a {@link Reiterable}.
      checkState(request.getGet().getContinuationToken().isEmpty(),
          "Continuation tokens are unsupported.");

      return CompletableFuture.completedFuture(
          StateResponse.newBuilder()
              .setId(request.getId())
              .setGet(StateGetResponse.newBuilder()
                  // Note that this doesn't copy the actual bytes, just the references.
                  .setData(ByteString.copyFrom(handler.get(key, window)))));
    }

    private static <W extends BoundedWindow> CompletionStage<StateResponse.Builder>
    handleAppendRequest(
        StateRequest request,
        ByteString key,
        W window,
        BagUserStateHandler<ByteString, ByteString, W> handler) {
      handler.append(key, window, ImmutableList.of(request.getAppend().getData()).iterator());
      return CompletableFuture.completedFuture(
          StateResponse.newBuilder()
              .setId(request.getId())
              .setAppend(StateAppendResponse.getDefaultInstance()));
    }

    private static <W extends BoundedWindow> CompletionStage<StateResponse.Builder>
    handleClearRequest(
        StateRequest request,
        ByteString key,
        W window,
        BagUserStateHandler<ByteString, ByteString, W> handler) {
      handler.clear(key, window);
      return CompletableFuture.completedFuture(
          StateResponse
              .newBuilder()
              .setId(request.getId())
              .setClear(StateClearResponse.getDefaultInstance()));
    }

    private <K, V, W extends BoundedWindow> BagUserStateHandler<K, V, W> createHandler(
        BagUserStateSpec cacheKey) {
      return handlerFactory.forUserState(
          cacheKey.transformId(),
          cacheKey.userStateId(),
          cacheKey.keyCoder(),
          cacheKey.valueCoder(),
          cacheKey.windowCoder());
    }
  }
}
