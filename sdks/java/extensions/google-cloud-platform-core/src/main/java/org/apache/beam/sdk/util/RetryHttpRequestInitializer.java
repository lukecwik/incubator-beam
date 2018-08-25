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
package org.apache.beam.sdk.util;

import static com.google.api.client.util.BackOffUtils.next;

import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a request initializer that adds retry handlers to all
 * HttpRequests.
 *
 * <p>Also can take a HttpResponseInterceptor to be applied to the responses.
 */
public class RetryHttpRequestInitializer implements HttpRequestInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(RetryHttpRequestInitializer.class);

  /**
   * Http response codes that should be silently ignored.
   */
  private static final Set<Integer> DEFAULT_IGNORED_RESPONSE_CODES = new HashSet<>(
      Arrays.asList(307 /* Redirect, handled by the client library */,
                    308 /* Resume Incomplete, handled by the client library */));

  /**
   * Http response timeout to use for hanging gets.
   */
  private static final int HANGING_GET_TIMEOUT_SEC = 80;

  /** Handlers used to provide additional logging information on unsuccessful HTTP requests. */
  private static class LoggingHttpBackOffHandler
      implements HttpIOExceptionHandler, HttpUnsuccessfulResponseHandler {

    private final Sleeper sleeper;
    private final BackOff ioExceptionBackOff;
    private final BackOff unsuccessfulResponseBackOff;
    private final Set<Integer> ignoredResponseCodes;
    private int ioExceptionRetries;
    private int unsuccessfulResponseRetries;

    private LoggingHttpBackOffHandler(
        Sleeper sleeper,
        BackOff ioExceptionBackOff,
        BackOff unsucessfulResponseBackOff,
        Set<Integer> ignoredResponseCodes) {
      this.sleeper = sleeper;
      this.ioExceptionBackOff = ioExceptionBackOff;
      this.unsuccessfulResponseBackOff = unsucessfulResponseBackOff;
      this.ignoredResponseCodes = ignoredResponseCodes;
    }

    @Override
    public boolean handleIOException(HttpRequest request, boolean supportsRetry)
        throws IOException {
      // We will retry if the request supports retry or the backoff was successful.
      // Note that the order of these checks is important since
      // backOffWasSuccessful will perform a sleep.
      boolean willRetry = supportsRetry;

      Exception backOffFailed = null;
      if (willRetry) {
        try {
          willRetry = next(sleeper, ioExceptionBackOff);
        } catch (IOException | InterruptedException e) {
          backOffFailed = e;
        }
      }

      if (willRetry) {
        ioExceptionRetries += 1;
        LOG.debug("Request failed with IOException, will retry: {}", request.getUrl());
      } else {
        String message = "Request failed with IOException, "
            + "performed {} retries due to IOExceptions, "
            + "performed {} retries due to unsuccessful status codes, "
            + "HTTP framework says request {} be retried, backoff {} [{}]"
            + "(caller responsible for retrying): {}";
        LOG.warn(message,
            ioExceptionRetries,
            unsuccessfulResponseRetries,
            supportsRetry ? "can" : "cannot",
            backOffFailed == null ? "gave up" : "failed due to exception",
            ioExceptionBackOff,
            request.getUrl(),
            backOffFailed);
        if (ioExceptionBackOff instanceof ExponentialBackOff) {
          ExponentialBackOff backOff = (ExponentialBackOff) ioExceptionBackOff;
          LOG.warn(MoreObjects.toStringHelper(backOff)
              .add("getCurrentIntervalMillis", backOff.getCurrentIntervalMillis())
              .add("getElapsedTimeMillis", backOff.getElapsedTimeMillis())
              .add("getInitialIntervalMillis", backOff.getInitialIntervalMillis())
              .add("getMaxElapsedTimeMillis", backOff.getMaxElapsedTimeMillis())
              .add("getMaxIntervalMillis", backOff.getMaxIntervalMillis())
              .add("getMultiplier", backOff.getMultiplier())
              .add("getRandomizationFactor", backOff.getRandomizationFactor())
              .add("nextBackOffMillis", backOff.nextBackOffMillis())
              .toString());
        }
      }
      return willRetry;
    }

    @Override
    public boolean handleResponse(HttpRequest request, HttpResponse response, boolean supportsRetry)
        throws IOException {
      // We will retry if the request supports retry and the status code requires a backoff
      // and the backoff was successful. Note that the order of these checks is important since
      // next will perform a sleep.
      boolean willRetry = supportsRetry && retryOnStatusCode(response.getStatusCode());
      Exception backOffFailed = null;
      if (willRetry) {
        try {
          willRetry = next(sleeper, unsuccessfulResponseBackOff);
        } catch (IOException | InterruptedException e) {
          backOffFailed = e;
        }
      }

      if (willRetry) {
        unsuccessfulResponseRetries += 1;
        LOG.debug("Request failed with code {}, will retry: {}",
            response.getStatusCode(), request.getUrl());
      } else {
        String message = "Request failed with code {}, "
            + "performed {} retries due to IOExceptions, "
            + "performed {} retries due to unsuccessful status codes, "
            + "HTTP framework says request {} be retried, backoff {} [{}]"
            + "(caller responsible for retrying): {}";
        if (ignoredResponseCodes.contains(response.getStatusCode())) {
          // Log ignored response codes at a lower level
          LOG.debug(message,
              response.getStatusCode(),
              ioExceptionRetries,
              unsuccessfulResponseRetries,
              supportsRetry ? "can" : "cannot",
              backOffFailed == null ? "gave up" : "failed due to exception",
              unsuccessfulResponseBackOff,
              request.getUrl(),
              backOffFailed);
          if (unsuccessfulResponseBackOff instanceof ExponentialBackOff) {
            ExponentialBackOff backOff = (ExponentialBackOff) unsuccessfulResponseBackOff;
            LOG.debug(MoreObjects.toStringHelper(backOff)
                .add("getCurrentIntervalMillis", backOff.getCurrentIntervalMillis())
                .add("getElapsedTimeMillis", backOff.getElapsedTimeMillis())
                .add("getInitialIntervalMillis", backOff.getInitialIntervalMillis())
                .add("getMaxElapsedTimeMillis", backOff.getMaxElapsedTimeMillis())
                .add("getMaxIntervalMillis", backOff.getMaxIntervalMillis())
                .add("getMultiplier", backOff.getMultiplier())
                .add("getRandomizationFactor", backOff.getRandomizationFactor())
                .add("nextBackOffMillis", backOff.nextBackOffMillis())
                .toString());
          }
        } else {
          LOG.warn(message,
              response.getStatusCode(),
              ioExceptionRetries,
              unsuccessfulResponseRetries,
              supportsRetry ? "can" : "cannot",
              backOffFailed == null ? "gave up" : "failed due to exception",
              unsuccessfulResponseBackOff,
              request.getUrl(),
              backOffFailed);
          if (unsuccessfulResponseBackOff instanceof ExponentialBackOff) {
            ExponentialBackOff backOff = (ExponentialBackOff) unsuccessfulResponseBackOff;
            LOG.warn(MoreObjects.toStringHelper(backOff)
                .add("getCurrentIntervalMillis", backOff.getCurrentIntervalMillis())
                .add("getElapsedTimeMillis", backOff.getElapsedTimeMillis())
                .add("getInitialIntervalMillis", backOff.getInitialIntervalMillis())
                .add("getMaxElapsedTimeMillis", backOff.getMaxElapsedTimeMillis())
                .add("getMaxIntervalMillis", backOff.getMaxIntervalMillis())
                .add("getMultiplier", backOff.getMultiplier())
                .add("getRandomizationFactor", backOff.getRandomizationFactor())
                .add("nextBackOffMillis", backOff.nextBackOffMillis())
                .toString());
          }
        }
      }
      return willRetry;
    }

    /** Returns true iff the {@code statusCode} represents an error that should be retried. */
    private boolean retryOnStatusCode(int statusCode) {
      return (statusCode == 0) // Code 0 usually means no response / network error
          || (statusCode / 100 == 5) // 5xx: server error
          || statusCode == 429; // 429: Too many requests
    }
  }

  private final HttpResponseInterceptor responseInterceptor;  // response Interceptor to use

  private final NanoClock nanoClock;  // used for testing

  private final Sleeper sleeper;  // used for testing

  private Set<Integer> ignoredResponseCodes = new HashSet<>(DEFAULT_IGNORED_RESPONSE_CODES);

  public RetryHttpRequestInitializer() {
    this(Collections.emptyList());
  }

  /**
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   */
  public RetryHttpRequestInitializer(Collection<Integer> additionalIgnoredResponseCodes) {
    this(additionalIgnoredResponseCodes, null);
  }

  /**
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   * @param responseInterceptor HttpResponseInterceptor to be applied on all requests. May be null.
   */
  public RetryHttpRequestInitializer(
      Collection<Integer> additionalIgnoredResponseCodes,
      @Nullable HttpResponseInterceptor responseInterceptor) {
    this(NanoClock.SYSTEM, Sleeper.DEFAULT, additionalIgnoredResponseCodes,
        responseInterceptor);
  }

  /**
   * Visible for testing.
   *
   * @param nanoClock used as a timing source for knowing how much time has elapsed.
   * @param sleeper used to sleep between retries.
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   */
  RetryHttpRequestInitializer(
      NanoClock nanoClock, Sleeper sleeper, Collection<Integer> additionalIgnoredResponseCodes,
      HttpResponseInterceptor responseInterceptor) {
    this.nanoClock = nanoClock;
    this.sleeper = sleeper;
    this.ignoredResponseCodes.addAll(additionalIgnoredResponseCodes);
    this.responseInterceptor = responseInterceptor;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    // Set a timeout for hanging-gets.
    // TODO: Do this exclusively for work requests.
    request.setReadTimeout(HANGING_GET_TIMEOUT_SEC * 1000);

    LoggingHttpBackOffHandler loggingHttpBackOffHandler = new LoggingHttpBackOffHandler(
        sleeper,
        // Back off on retryable http errors and IOExceptions.
        // A back-off multiplier of 2 raises the maximum request retrying time
        // to approximately 5 minutes (keeping other back-off parameters to
        // their default values).
        new ExponentialBackOff.Builder().setNanoClock(nanoClock).setMultiplier(2).build(),
        new ExponentialBackOff.Builder().setNanoClock(nanoClock).setMultiplier(2).build(),
        ignoredResponseCodes
    );

    request.setUnsuccessfulResponseHandler(loggingHttpBackOffHandler);
    request.setIOExceptionHandler(loggingHttpBackOffHandler);

    // Set response initializer
    if (responseInterceptor != null) {
      request.setResponseInterceptor(responseInterceptor);
    }
  }
}
