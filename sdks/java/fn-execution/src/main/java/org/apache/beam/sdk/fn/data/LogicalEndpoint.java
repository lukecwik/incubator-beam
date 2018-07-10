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

import com.google.auto.value.AutoValue;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;

/**
 * A logical endpoint is a pair of an instruction ID corresponding to the {@link
 * BeamFnApi.ProcessBundleRequest} and the primitive transform within the processing graph. This
 * enables the same data service or data client to be re-used across multiple bundles.
 */
@AutoValue
public abstract class LogicalEndpoint {

  public abstract String getInstructionId();

  public abstract String getPrimitiveTransformReference();

  /** @deprecated Use {@link #of(String, String)}. */
  @Deprecated
  public static LogicalEndpoint of(String instructionId, BeamFnApi.Target target) {
    return new AutoValue_LogicalEndpoint(instructionId, target.getPrimitiveTransformReference());
  }

  public static LogicalEndpoint of(String instructionId, String primitiveTransformReference) {
    return new AutoValue_LogicalEndpoint(instructionId, primitiveTransformReference);
  }
}
