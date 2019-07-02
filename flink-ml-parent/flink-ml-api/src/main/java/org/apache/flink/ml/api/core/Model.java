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

package org.apache.flink.ml.api.core;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.Table;

/**
 * A model is an ordinary {@link Transformer} except how it is created. While ordinary transformers
 * are defined by specifying the parameters directly, a model is usually generated by an {@link
 * Estimator} when {@link Estimator#fit(org.apache.flink.table.api.TableEnvironment, Table)} is
 * invoked.
 *
 * <p>We separate Model from {@link Transformer} in order to support potential
 * model specific logic such as linking a Model to the {@link Estimator} from which the model was
 * generated.
 *
 * @param <M> The class type of the Model implementation itself, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams}
 */
@PublicEvolving
public interface Model<M extends Model<M>> extends Transformer<M> {
}
