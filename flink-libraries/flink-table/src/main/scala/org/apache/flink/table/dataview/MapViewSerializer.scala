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

package org.apache.flink.table.dataview

import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.common.typeutils.base.{MapSerializer, MapSerializerConfigSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.table.api.dataview.MapView

/**
  * A serializer for [[MapView]]. The serializer relies on a key serializer and a value
  * serializer for the serialization of the map's key-value pairs.
  *
  * The serialization format for the map is as follows: four bytes for the length of the map,
  * followed by the serialized representation of each key-value pair. To allow null values,
  * each value is prefixed by a null marker.
  *
  * @param mapSerializer Map serializer.
  * @tparam K The type of the keys in the map.
  * @tparam V The type of the values in the map.
  */
class MapViewSerializer[K, V](val mapSerializer: MapSerializer[K, V])
  extends TypeSerializer[MapView[K, V]] {

  override def isImmutableType: Boolean = false

  override def duplicate(): TypeSerializer[MapView[K, V]] =
    new MapViewSerializer[K, V](
      mapSerializer.duplicate().asInstanceOf[MapSerializer[K, V]])

  override def createInstance(): MapView[K, V] = {
    new MapView[K, V]()
  }

  override def copy(from: MapView[K, V]): MapView[K, V] = {
    new MapView[K, V](null, null, mapSerializer.copy(from.map))
  }

  override def copy(from: MapView[K, V], reuse: MapView[K, V]): MapView[K, V] = copy(from)

  override def getLength: Int = -1  // var length

  override def serialize(record: MapView[K, V], target: DataOutputView): Unit = {
    mapSerializer.serialize(record.map, target)
  }

  override def deserialize(source: DataInputView): MapView[K, V] = {
    new MapView[K, V](null, null, mapSerializer.deserialize(source))
  }

  override def deserialize(reuse: MapView[K, V], source: DataInputView): MapView[K, V] =
    deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit =
    mapSerializer.copy(source, target)

  override def canEqual(obj: Any): Boolean = obj != null && obj.getClass == getClass

  override def hashCode(): Int = mapSerializer.hashCode()

  override def equals(obj: Any): Boolean = canEqual(this) &&
    mapSerializer.equals(obj.asInstanceOf[MapViewSerializer[_, _]].mapSerializer)

  override def snapshotConfiguration(): MapViewSerializerConfigSnapshot[K, V] =
    new MapViewSerializerConfigSnapshot[K, V](mapSerializer)

  // copy and modified from MapSerializer.ensureCompatibility
  override def ensureCompatibility(configSnapshot: TypeSerializerConfigSnapshot[_])
  : TypeSerializerSchemaCompatibility[MapView[K, V]] = {

    configSnapshot match {
      case snapshot: MapViewSerializerConfigSnapshot[K, V] =>
        checkCompatibility(snapshot)

      // backwards compatibility path;
      // Flink versions older or equal to 1.5.x returns a
      // MapSerializerConfigSnapshot as the snapshot
      case legacySnapshot: MapSerializerConfigSnapshot[K, V] =>
        checkCompatibility(legacySnapshot)

      case _ => TypeSerializerSchemaCompatibility.incompatible()
    }
  }

  private def checkCompatibility(
      configSnapshot: CompositeTypeSerializerConfigSnapshot[_]
    ): TypeSerializerSchemaCompatibility[MapView[K, V]] = {

    val previousKvSerializersAndConfigs = configSnapshot.getNestedSerializersAndConfigs

    val keyCompatResult = CompatibilityUtil.resolveCompatibilityResult(
      previousKvSerializersAndConfigs.get(0).f1,
      mapSerializer.getKeySerializer)

    val valueCompatResult = CompatibilityUtil.resolveCompatibilityResult(
      previousKvSerializersAndConfigs.get(1).f1,
      mapSerializer.getValueSerializer)

    if (!keyCompatResult.isIncompatible && !valueCompatResult.isIncompatible) {
      TypeSerializerSchemaCompatibility.compatibleAsIs()
    } else {
      TypeSerializerSchemaCompatibility.incompatible()
    }
  }
}
