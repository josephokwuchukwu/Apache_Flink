/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.BroadcastStateTransformation;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link BroadcastStateTransformation}.
 *
 * @param <IN1> The type of the elements in the non-broadcasted input of the {@link BroadcastStateTransformation}.
 * @param <IN2> The type of the elements in the broadcasted input of the {@link BroadcastStateTransformation}.
 * @param <OUT> The type of the elements that result from the {@link BroadcastStateTransformation}.
 */
@Internal
public class BroadcastStateTransformationTranslator<IN1, IN2, OUT>
		extends SimpleTransformationTranslator<OUT, BroadcastStateTransformation<IN1, IN2, OUT>>  {

	@Override
	protected Collection<Integer> translateForBatchInternal(
			final BroadcastStateTransformation<IN1, IN2, OUT> transformation,
			final Context context) {
		throw new UnsupportedOperationException("The Broadcast State Pattern is not support in BATCH execution mode.");
	}

	@Override
	protected Collection<Integer> translateForStreamingInternal(
			final BroadcastStateTransformation<IN1, IN2, OUT> transformation,
			final Context context) {
		checkNotNull(transformation);
		checkNotNull(context);

		final TypeInformation<IN1> nonBroadcastTypeInfo =
				transformation.getNonBroadcastStream().getType();
		final TypeInformation<IN2> broadcastTypeInfo =
				transformation.getBroadcastStream().getType();

		final StreamGraph streamGraph = context.getStreamGraph();
		final String slotSharingGroup = context.getSlotSharingGroup();
		final int transformationId = transformation.getId();
		final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

		streamGraph.addCoOperator(
				transformationId,
				slotSharingGroup,
				transformation.getCoLocationGroupKey(),
				transformation.getOperatorFactory(),
				nonBroadcastTypeInfo,
				broadcastTypeInfo,
				transformation.getOutputType(),
				transformation.getName());

		if (transformation.getKeySelector() != null) {
			final TypeSerializer<?> keySerializer =
					transformation.getStateKeyType().createSerializer(executionConfig);

			streamGraph.setTwoInputStateKey(
					transformationId,
					transformation.getKeySelector(),
					null,
					keySerializer);
		}

		final int parallelism = transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
				? transformation.getParallelism()
				: executionConfig.getParallelism();
		streamGraph.setParallelism(transformationId, parallelism);
		streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

		for (Integer inputId: context.getStreamNodeIds(transformation.getNonBroadcastStream().getTransformation())) {
			streamGraph.addEdge(inputId, transformationId, 1);
		}

		for (Integer inputId: context.getStreamNodeIds(transformation.getBroadcastStream().getTransformation())) {
			streamGraph.addEdge(inputId, transformationId, 2);
		}

		return Collections.singleton(transformationId);

	}
}
