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
package org.apache.beam.runners.core.construction.graph;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/**
 * Validates well-formedness of a pipeline. It is recommended to use this class on any user-supplied
 * Pipeline protos, and after any transformations on the pipeline, to verify that the
 * transformations didn't break well-formedness.
 */
public class PipelineValidator {
  @FunctionalInterface
  private interface TransformValidator {
    void validate(String transformId, PTransform transform, Components components, Pipeline p)
        throws Exception;
  }

  private static final ImmutableMap<String, TransformValidator> VALIDATORS =
      ImmutableMap.<String, TransformValidator>builder()
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, PipelineValidator::validateParDo)
          // Nothing to validate for FLATTEN, GROUP_BY_KEY, IMPULSE
          .put(
              PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN,
              PipelineValidator::validateAssignWindows)
          .put(
              PTransformTranslation.TEST_STREAM_TRANSFORM_URN,
              PipelineValidator::validateTestStream)
          // Nothing to validate for MAP_WINDOWS, READ, CREATE_VIEW.
          .put(
              PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          .put(
              PTransformTranslation.COMBINE_GLOBALLY_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          // Nothing to validate for RESHUFFLE and WRITE_FILES
          .put(
              PTransformTranslation.COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          .put(
              PTransformTranslation.COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          .put(
              PTransformTranslation.COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          .put(
              PTransformTranslation.COMBINE_GROUPED_VALUES_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          .put(
              PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
              PipelineValidator::validateParDo)
          .put(
              PTransformTranslation.SPLITTABLE_SPLIT_RESTRICTION_URN,
              PipelineValidator::validateParDo)
          .put(PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN, PipelineValidator::validateParDo)
          .put(ExecutableStage.URN, PipelineValidator::validateExecutableStage)
          .build();

  public static void validate(Pipeline p) {
    Components components = p.getComponents();

    for (String transformId : p.getRootTransformIdsList()) {
      checkArgument(
          components.containsTransforms(transformId),
          "Root transform id %s is unknown",
          transformId);
    }

    validateComponents("pipeline", components, p);
  }

  private static void validateComponents(String context, Components components, Pipeline p) {
    {
      Map<String, String> uniqueNamesById = Maps.newHashMap();
      for (String transformId : components.getTransformsMap().keySet()) {
        PTransform transform = components.getTransformsOrThrow(transformId);
        String previousId = uniqueNamesById.put(transform.getUniqueName(), transformId);
        // A transform is allowed to not have unique_name set, but, obviously,
        // there can be only one such transform with an empty name.
        // It's allowed for the (only) root transform to have the empty unique_name.
        checkArgument(
            previousId == null,
            "%s: Transforms %s and %s both have unique_name \"%s\"",
            context,
            transformId,
            previousId,
            transform.getUniqueName());
        validateTransform(transformId, transform, components, p);
      }
    }
    {
      Map<String, String> uniqueNamesById = Maps.newHashMap();
      for (String pcollectionId : components.getPcollectionsMap().keySet()) {
        PCollection pc = components.getPcollectionsOrThrow(pcollectionId);
        checkArgument(
            !pc.getUniqueName().isEmpty(),
            "%s: PCollection %s does not have a unique_name set",
            context,
            pcollectionId);
        String previousId = uniqueNamesById.put(pc.getUniqueName(), pcollectionId);
        checkArgument(
            previousId == null,
            "%s: PCollections %s and %s both have unique_name \"%s\"",
            context,
            pcollectionId,
            previousId,
            pc.getUniqueName());
        checkArgument(
            components.containsCoders(pc.getCoderId()),
            "%s: PCollection %s uses unknown coder %s",
            context,
            pcollectionId,
            pc.getCoderId());
        checkArgument(
            components.containsWindowingStrategies(pc.getWindowingStrategyId()),
            "%s: PCollection %s uses unknown windowing strategy %s",
            context,
            pcollectionId,
            pc.getWindowingStrategyId());
      }
    }

    for (String strategyId : components.getWindowingStrategiesMap().keySet()) {
      WindowingStrategy strategy = components.getWindowingStrategiesOrThrow(strategyId);
      checkArgument(
          components.containsCoders(strategy.getWindowCoderId()),
          "%s: WindowingStrategy %s uses unknown coder %s",
          context,
          strategyId,
          strategy.getWindowCoderId());
    }

    for (String coderId : components.getCodersMap().keySet()) {
      for (String componentCoderId :
          components.getCodersOrThrow(coderId).getComponentCoderIdsList()) {
        checkArgument(
            components.containsCoders(componentCoderId),
            "%s: Coder %s uses unknown component coder %s",
            context,
            coderId,
            componentCoderId);
      }
    }
  }

  private static void validateTransform(
      String id, PTransform transform, Components components, Pipeline p) {
    if (!transform.getEnvironmentId().isEmpty()) {
      checkArgument(
          components.containsEnvironments(transform.getEnvironmentId()),
          "Transform %s references unknown environment %s",
          id,
          transform.getEnvironmentId());
    }

    for (String subtransformId : transform.getSubtransformsList()) {
      checkArgument(
          components.containsTransforms(subtransformId),
          "Transform %s references unknown subtransform %s",
          id,
          subtransformId);
    }

    for (Map.Entry<String, String> nameToPCollectionId : transform.getInputsMap().entrySet()) {
      checkArgument(
          components.containsPcollections(nameToPCollectionId.getValue()),
          "Transform %s input %s points to unknown PCollection %s",
          id,
          nameToPCollectionId.getKey(),
          nameToPCollectionId.getValue());
    }
    for (Map.Entry<String, String> nameToPCollectionId : transform.getOutputsMap().entrySet()) {
      checkArgument(
          components.containsPcollections(nameToPCollectionId.getValue()),
          "Transform %s output %s points to unknown PCollection %s",
          id,
          nameToPCollectionId.getKey(),
          nameToPCollectionId.getValue());
    }

    String urn = transform.getSpec().getUrn();
    if (VALIDATORS.containsKey(urn)) {
      try {
        VALIDATORS.get(urn).validate(id, transform, components, p);
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to validate transform %s", id), e);
      }
    }
  }

  private static final Set<String> SUPPORTED_MATERIALIZATION_FORMATS =
      ImmutableSet.of(
          Materializations.ITERABLE_MATERIALIZATION_URN,
          Materializations.MULTIMAP_MATERIALIZATION_URN);

  private static void validateParDo(
      String id, PTransform transform, Components components, Pipeline p) throws Exception {
    ParDoPayload payload = ParDoPayload.parseFrom(transform.getSpec().getPayload());

    checkNotNull(
        transform.getEnvironmentId(), "Transform %s does not have an environment specified", id);

    // side_inputs
    for (Map.Entry<String, SideInput> nameToSideInput : payload.getSideInputsMap().entrySet()) {
      checkArgument(
          transform.containsInputs(nameToSideInput.getKey()),
          "Transform %s side input %s is not listed in the transform's inputs",
          id,
          nameToSideInput.getKey());
      checkArgument(
          SUPPORTED_MATERIALIZATION_FORMATS.contains(nameToSideInput.getValue().getAccessPattern()),
          "Transform %s side input %s declares unknown side input materialization format %s, must be one of %s",
          id,
          nameToSideInput.getKey(),
          nameToSideInput.getValue().getAccessPattern().getUrn(),
          SUPPORTED_MATERIALIZATION_FORMATS);
    }

    // state & timers
    if (!payload.getStateSpecsMap().isEmpty()
        || !payload.getTimerSpecsMap().isEmpty()
        || !payload.getTimerFamilySpecsMap().isEmpty()) {
      checkArgument(
          p.getRequirementsList().contains(ParDoTranslation.REQUIRES_STATEFUL_PROCESSING_URN),
          "Transform %s uses stateful processing but pipeline does not declare requirement %s",
          id,
          ParDoTranslation.REQUIRES_SPLITTABLE_DOFN_URN);
    }
    // TODO: StateSpec, TimerSpec, TimerFamilySpec

    if (!payload.getRestrictionCoderId().isEmpty()) {
      checkArgument(
          p.getRequirementsList().contains(ParDoTranslation.REQUIRES_SPLITTABLE_DOFN_URN),
          "Transform %s uses splittable dofn but pipeline does not declare requirement %s",
          id,
          ParDoTranslation.REQUIRES_SPLITTABLE_DOFN_URN);
      checkArgument(
          components.containsCoders(payload.getRestrictionCoderId()),
          "Transform %s uses unknown restriction coder id %s",
          id,
          payload.getRestrictionCoderId());
    }
    if (payload.getRequestsFinalization()) {
      checkArgument(
          p.getRequirementsList().contains(ParDoTranslation.REQUIRES_BUNDLE_FINALIZATION_URN),
          "Transform %s requests finalization but pipeline does not declare requirement %s",
          id,
          ParDoTranslation.REQUIRES_BUNDLE_FINALIZATION_URN);
    }
    if (payload.getRequiresStableInput()) {
      checkArgument(
          p.getRequirementsList().contains(ParDoTranslation.REQUIRES_STABLE_INPUT_URN),
          "Transform %s requires stable input but pipeline does not declare requirement %s",
          id,
          ParDoTranslation.REQUIRES_STABLE_INPUT_URN);
    }
    if (payload.getRequiresTimeSortedInput()) {
      checkArgument(
          p.getRequirementsList().contains(ParDoTranslation.REQUIRES_TIME_SORTED_INPUT_URN),
          "Transform %s requires time sorted input but pipeline does not declare requirement %s",
          id,
          ParDoTranslation.REQUIRES_TIME_SORTED_INPUT_URN);
    }
  }

  private static void validateAssignWindows(
      String id, PTransform transform, Components components, Pipeline p) throws Exception {
    WindowIntoPayload.parseFrom(transform.getSpec().getPayload());
  }

  private static void validateTestStream(
      String id, PTransform transform, Components components, Pipeline p) throws Exception {
    TestStreamPayload.parseFrom(transform.getSpec().getPayload());
  }

  private static void validateCombine(
      String id, PTransform transform, Components components, Pipeline p) throws Exception {
    CombinePayload payload = CombinePayload.parseFrom(transform.getSpec().getPayload());
    checkArgument(
        components.containsCoders(payload.getAccumulatorCoderId()),
        "Transform %s uses unknown accumulator coder id %s",
        payload.getAccumulatorCoderId());
  }

  private static void validateExecutableStage(
      String id, PTransform transform, Components outerComponents, Pipeline p) throws Exception {
    ExecutableStagePayload payload =
        ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());

    // Everything within an ExecutableStagePayload uses only the stage's components.
    Components components = payload.getComponents();

    checkArgument(
        transform.getInputsMap().values().contains(payload.getInput()),
        "ExecutableStage %s uses unknown input %s",
        id,
        payload.getInput());

    checkArgument(
        !payload.getTransformsList().isEmpty(), "ExecutableStage %s contains no transforms", id);

    for (String subtransformId : payload.getTransformsList()) {
      checkArgument(
          components.containsTransforms(subtransformId),
          "ExecutableStage %s uses unknown transform %s",
          id,
          subtransformId);
    }
    for (String outputId : payload.getOutputsList()) {
      checkArgument(
          components.containsPcollections(outputId),
          "ExecutableStage %s uses unknown output %s",
          id,
          outputId);
    }

    validateComponents("ExecutableStage " + id, components, p);

    // TODO: Also validate that side inputs of all transforms within components.getTransforms()
    // are contained within payload.getSideInputsList()
  }
}
