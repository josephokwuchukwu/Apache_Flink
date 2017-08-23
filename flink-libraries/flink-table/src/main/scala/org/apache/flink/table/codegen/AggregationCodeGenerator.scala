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
package org.apache.flink.table.codegen

import java.lang.reflect.{Modifier, ParameterizedType}
import java.lang.{Iterable => JIterable}

import org.apache.flink.api.common.state.StateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.dataview.{ListView, MapView}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.CodeGenUtils.{newName, reflectiveFieldWriteAccess}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getUserDefinedMethod, signatureToString}
import org.apache.flink.table.runtime.aggregate.{AggregateUtil, DataViewConfig, GeneratedAggregations, SingleElementIterable}

import scala.collection.mutable

/**
  * A code generator for generating [[GeneratedAggregations]].
  *
  * @param config configuration that determines runtime behavior
  * @param nullableInput input(s) can be null.
  * @param input type information about the input of the Function
  */
class AggregationCodeGenerator(
    config: TableConfig,
    nullableInput: Boolean,
    input: TypeInformation[_ <: Any])
  extends CodeGenerator(config, nullableInput, input) {

  // set of statements for cleanup dataview that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableCleanupStatements = mutable.LinkedHashSet[String]()

  /**
    * @return code block of statements that need to be placed in the cleanup() method of
    *         RichFunction
    */
  def reuseCleanupCode(): String = {
    reusableCleanupStatements.mkString("", "\n", "\n")
  }

  /**
    * Generates a [[org.apache.flink.table.runtime.aggregate.GeneratedAggregations]] that can be
    * passed to a Java compiler.
    *
    * @param name        Class name of the function.
    *                    Does not need to be unique but has to be a valid Java class identifier.
    * @param physicalInputTypes Physical input row types
    * @param aggregates  All aggregate functions
    * @param aggFields   Indexes of the input fields for all aggregate functions
    * @param aggMapping  The mapping of aggregates to output fields
    * @param partialResults A flag defining whether final or partial results (accumulators) are set
    *                       to the output row.
    * @param fwdMapping  The mapping of input fields to output fields
    * @param mergeMapping An optional mapping to specify the accumulators to merge. If not set, we
    *                     assume that both rows have the accumulators at the same position.
    * @param constantFlags An optional parameter to define where to set constant boolean flags in
    *                      the output row.
    * @param outputArity The number of fields in the output row.
    * @param needRetract a flag to indicate if the aggregate needs the retract method
    * @param needMerge a flag to indicate if the aggregate needs the merge method
    * @param needReset a flag to indicate if the aggregate needs the resetAccumulator method
    *
    * @return A GeneratedAggregationsFunction
    */
  def generateAggregations(
    name: String,
    physicalInputTypes: Seq[TypeInformation[_]],
    aggregates: Array[AggregateFunction[_ <: Any, _ <: Any]],
    aggFields: Array[Array[Int]],
    aggMapping: Array[Int],
    partialResults: Boolean,
    fwdMapping: Array[Int],
    mergeMapping: Option[Array[Int]],
    constantFlags: Option[Array[(Int, Boolean)]],
    outputArity: Int,
    needRetract: Boolean,
    needMerge: Boolean,
    needReset: Boolean,
    accConfig: DataViewConfig)
  : GeneratedAggregationsFunction = {

    // get unique function name
    val funcName = newName(name)
    // register UDAGGs
    val aggs = aggregates.map(a => addReusableFunction(a, contextTerm))

    // get java types of accumulators
    val accTypeClasses = aggregates.map { a =>
      a.getClass.getMethod("createAccumulator").getReturnType
    }
    val accTypes = accTypeClasses.map(_.getCanonicalName)

    // get parameter lists for aggregation functions
    val parametersCode = aggFields.map { inFields =>
      val fields = for (f <- inFields) yield
        s"(${CodeGenUtils.boxedTypeTermForTypeInfo(physicalInputTypes(f))}) input.getField($f)"
      fields.mkString(", ")
    }

    // get method signatures
    val classes = UserDefinedFunctionUtils.typeInfoToClass(physicalInputTypes)
    val methodSignaturesList = aggFields.map { inFields =>
      inFields.map(classes(_))
    }

    // initialize and create data views
    addReusableDataViews

    // check and validate the needed methods
    aggregates.zipWithIndex.map {
      case (a, i) =>
        getUserDefinedMethod(a, "accumulate", Array(accTypeClasses(i)) ++ methodSignaturesList(i))
        .getOrElse(
          throw new CodeGenException(
            s"No matching accumulate method found for AggregateFunction " +
              s"'${a.getClass.getCanonicalName}'" +
              s"with parameters '${signatureToString(methodSignaturesList(i))}'.")
        )

        if (needRetract) {
          getUserDefinedMethod(a, "retract", Array(accTypeClasses(i)) ++ methodSignaturesList(i))
          .getOrElse(
            throw new CodeGenException(
              s"No matching retract method found for AggregateFunction " +
                s"'${a.getClass.getCanonicalName}'" +
                s"with parameters '${signatureToString(methodSignaturesList(i))}'.")
          )
        }

        if (needMerge) {
          val methods =
            getUserDefinedMethod(a, "merge", Array(accTypeClasses(i), classOf[JIterable[Any]]))
            .getOrElse(
              throw new CodeGenException(
                s"No matching merge method found for AggregateFunction " +
                  s"${a.getClass.getCanonicalName}'.")
            )

          var iterableTypeClass = methods.getGenericParameterTypes.apply(1)
                                  .asInstanceOf[ParameterizedType].getActualTypeArguments.apply(0)
          // further extract iterableTypeClass if the accumulator has generic type
          iterableTypeClass match {
            case impl: ParameterizedType => iterableTypeClass = impl.getRawType
            case _ =>
          }

          if (iterableTypeClass != accTypeClasses(i)) {
            throw new CodeGenException(
              s"merge method in AggregateFunction ${a.getClass.getCanonicalName} does not have " +
                s"the correct Iterable type. Actually: ${iterableTypeClass.toString}. " +
                s"Expected: ${accTypeClasses(i).toString}")
          }
        }

        if (needReset) {
          getUserDefinedMethod(a, "resetAccumulator", Array(accTypeClasses(i)))
          .getOrElse(
            throw new CodeGenException(
              s"No matching resetAccumulator method found for " +
                s"aggregate ${a.getClass.getCanonicalName}'.")
          )
        }
    }

    /**
      * Adds a reusable [[org.apache.flink.table.api.dataview.DataView]] to the open, cleanup,
      * close and member area of the generated function.
      *
      */
    def addReusableDataViews: Unit = {
      if (accConfig != null && accConfig.isStateBackedDataViews) {
        val descMapping: Map[String, StateDescriptor[_, _]] = accConfig.accSpecs
          .flatMap(specs => specs.map(s => (s.id, s.toStateDescriptor)))
          .toMap[String, StateDescriptor[_, _]]

        for (i <- aggs.indices) yield {
          for (spec <- accConfig.accSpecs(i)) yield {
            val dataViewField = spec.field
            val dataViewTypeTerm = dataViewField.getType.getCanonicalName
            val desc = descMapping.getOrElse(spec.id,
              throw new CodeGenException(s"Can not find ListView in accumulator by id: ${spec.id}"))

            // define the DataView variables
            val serializedData = AggregateUtil.serialize(desc)
            val dataViewFieldTerm = AggregateUtil.createDataViewTerm(i, dataViewField.getName)
            val field =
              s"""
                 |transient $dataViewTypeTerm $dataViewFieldTerm = null;
                 |""".stripMargin
            reusableMemberStatements.add(field)

            // create DataViews
            val descFieldTerm = s"${dataViewFieldTerm}_desc"
            val descClassQualifier = classOf[StateDescriptor[_, _]].getCanonicalName
            val descDeserialize =
              s"""
                 |    $descClassQualifier $descFieldTerm = ($descClassQualifier)
                 |      ${AggregateUtil.getClass.getName.stripSuffix("$")}
                 |      .deserialize("$serializedData");
             """.stripMargin
            val createDataView = if (dataViewField.getType == classOf[MapView[_, _]]) {
              s"""
                 |    $descDeserialize
                 |    $dataViewFieldTerm = new org.apache.flink.table.dataview.StateMapView(
                 |      $contextTerm.getMapState((
                 |        org.apache.flink.api.common.state.MapStateDescriptor)$descFieldTerm));
           """.stripMargin
            } else if (dataViewField.getType == classOf[ListView[_]]) {
              s"""
                 |    $descDeserialize
                 |    $dataViewFieldTerm = new org.apache.flink.table.dataview.StateListView(
                 |      $contextTerm.getListState((
                 |        org.apache.flink.api.common.state.ListStateDescriptor)$descFieldTerm));
           """.stripMargin
            } else {
              throw new CodeGenException(s"Unsupported dataview type: $dataViewTypeTerm")
            }
            reusableOpenStatements.add(createDataView)

            // cleanup DataViews
            val cleanup =
              s"""
                 |    $dataViewFieldTerm.clear();
              """.stripMargin
            reusableCleanupStatements.add(cleanup)
          }
        }
      }
    }

    /**
      * Generate statements to set data view field when use state backend.
      *
      * @param accTerm aggregation term
      * @param aggIndex index of aggregation
      * @return data view field set statements
      */
    def genDataViewFieldSetter(accTerm: String, aggIndex: Int): String = {
      if (accConfig != null && accConfig.isStateBackedDataViews) {
        val setters = for (spec <- accConfig.accSpecs(aggIndex)) yield {
          val field = spec.field
          val dataViewTerm = AggregateUtil.createDataViewTerm(aggIndex, field.getName)
          val fieldSetter = if (Modifier.isPublic(field.getModifiers)) {
            s"$accTerm.${field.getName} = $dataViewTerm;"
          } else {
            val fieldTerm = addReusablePrivateFieldAccess(field.getDeclaringClass, field.getName)
            s"${reflectiveFieldWriteAccess(fieldTerm, field, accTerm, dataViewTerm)};"
          }

          s"""
             |    $fieldSetter
          """.stripMargin
        }
        setters.mkString("\n")
      } else {
        ""
      }
    }

    def genSetAggregationResults: String = {

      val sig: String =
        j"""
           |  public final void setAggregationResults(
           |    org.apache.flink.types.Row accs,
           |    org.apache.flink.types.Row output) throws Exception """.stripMargin

      val setAggs: String = {
        for (i <- aggs.indices) yield

          if (partialResults) {
            j"""
               |    output.setField(
               |      ${aggMapping(i)},
               |      (${accTypes(i)}) accs.getField($i));""".stripMargin
          } else {
            val setDataView = genDataViewFieldSetter(s"acc$i", i)
            j"""
               |    org.apache.flink.table.functions.AggregateFunction baseClass$i =
               |      (org.apache.flink.table.functions.AggregateFunction) ${aggs(i)};
               |    ${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
               |    $setDataView
               |    output.setField(
               |      ${aggMapping(i)},
               |      baseClass$i.getValue(acc$i));""".stripMargin
          }
      }.mkString("\n")

      j"""
         |$sig {
         |$setAggs
         |  }""".stripMargin
    }

    def genAccumulate: String = {

      val sig: String =
        j"""
           |  public final void accumulate(
           |    org.apache.flink.types.Row accs,
           |    org.apache.flink.types.Row input) throws Exception """.stripMargin

      val accumulate: String = {
        for (i <- aggs.indices) yield {
          val setDataView = genDataViewFieldSetter(s"acc$i", i)
          j"""
             |    ${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
             |    $setDataView
             |    ${aggs(i)}.accumulate(
             |      acc$i,
             |      ${parametersCode(i)});""".stripMargin
        }
      }.mkString("\n")

      j"""$sig {
         |$accumulate
         |  }""".stripMargin
    }

    def genRetract: String = {

      val sig: String =
        j"""
           |  public final void retract(
           |    org.apache.flink.types.Row accs,
           |    org.apache.flink.types.Row input) throws Exception """.stripMargin

      val retract: String = {
        for (i <- aggs.indices) yield {
          val setDataView = genDataViewFieldSetter(s"acc$i", i)
          j"""
             |    ${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
             |    $setDataView
             |    ${aggs(i)}.retract(
             |      acc$i,
             |      ${parametersCode(i)});""".stripMargin
        }
      }.mkString("\n")

      if (needRetract) {
        j"""
           |$sig {
           |$retract
           |  }""".stripMargin
      } else {
        j"""
           |$sig {
           |  }""".stripMargin
      }
    }

    def genCreateAccumulators: String = {

      val sig: String =
        j"""
           |  public final org.apache.flink.types.Row createAccumulators() throws Exception
           |    """.stripMargin
      val init: String =
        j"""
           |      org.apache.flink.types.Row accs =
           |          new org.apache.flink.types.Row(${aggs.length});"""
        .stripMargin
      val create: String = {
        for (i <- aggs.indices) yield {
          val setDataView = genDataViewFieldSetter(s"acc$i", i)
          j"""
             |    ${accTypes(i)} acc$i = (${accTypes(i)}) ${aggs(i)}.createAccumulator();
             |    $setDataView
             |    accs.setField(
             |      $i,
             |      acc$i);"""
            .stripMargin
        }
      }.mkString("\n")
      val ret: String =
        j"""
           |      return accs;"""
        .stripMargin

      j"""$sig {
         |$init
         |$create
         |$ret
         |  }""".stripMargin
    }

    def genSetForwardedFields: String = {

      val sig: String =
        j"""
           |  public final void setForwardedFields(
           |    org.apache.flink.types.Row input,
           |    org.apache.flink.types.Row output)
           |    """.stripMargin

      val forward: String = {
        for (i <- fwdMapping.indices if fwdMapping(i) >= 0) yield
          {
            j"""
               |    output.setField(
               |      $i,
               |      input.getField(${fwdMapping(i)}));"""
            .stripMargin
          }
      }.mkString("\n")

      j"""$sig {
         |$forward
         |  }""".stripMargin
    }

    def genSetConstantFlags: String = {

      val sig: String =
        j"""
           |  public final void setConstantFlags(org.apache.flink.types.Row output)
           |    """.stripMargin

      val setFlags: String = if (constantFlags.isDefined) {
        {
          for (cf <- constantFlags.get) yield {
            j"""
               |    output.setField(${cf._1}, ${if (cf._2) "true" else "false"});"""
            .stripMargin
          }
        }.mkString("\n")
      } else {
        ""
      }

      j"""$sig {
         |$setFlags
         |  }""".stripMargin
    }

    def genCreateOutputRow: String = {
      j"""
         |  public final org.apache.flink.types.Row createOutputRow() {
         |    return new org.apache.flink.types.Row($outputArity);
         |  }""".stripMargin
    }

    def genMergeAccumulatorsPair: String = {

      val mapping = mergeMapping.getOrElse(aggs.indices.toArray)

      val sig: String =
        j"""
           |  public final org.apache.flink.types.Row mergeAccumulatorsPair(
           |    org.apache.flink.types.Row a,
           |    org.apache.flink.types.Row b)
           """.stripMargin
      val merge: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${accTypes(i)} aAcc$i = (${accTypes(i)}) a.getField($i);
             |    ${accTypes(i)} bAcc$i = (${accTypes(i)}) b.getField(${mapping(i)});
             |    accIt$i.setElement(bAcc$i);
             |    ${aggs(i)}.merge(aAcc$i, accIt$i);
             |    a.setField($i, aAcc$i);
          """.stripMargin
      }.mkString("\n")
      val ret: String =
        j"""
           |      return a;
           """.stripMargin

      if (needMerge) {
        if (accConfig != null && accConfig.isStateBackedDataViews) {
          throw new CodeGenException("DataView doesn't support merge when the backend uses state.")
        }
        j"""
           |$sig {
           |$merge
           |$ret
           |  }""".stripMargin
      } else {
        j"""
           |$sig {
           |$ret
           |  }""".stripMargin
      }
    }

    def genMergeList: String = {
      {
        val singleIterableClass = classOf[SingleElementIterable[_]].getCanonicalName
        for (i <- accTypes.indices) yield
          j"""
             |    private final $singleIterableClass<${accTypes(i)}> accIt$i =
             |      new $singleIterableClass<${accTypes(i)}>();
             """.stripMargin
      }.mkString("\n")
    }

    def genResetAccumulator: String = {

      val sig: String =
        j"""
           |  public final void resetAccumulator(
           |    org.apache.flink.types.Row accs) throws Exception """.stripMargin

      val reset: String = {
        for (i <- aggs.indices) yield {
          val setDataView = genDataViewFieldSetter(s"acc$i", i)
          j"""
             |    ${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
             |    $setDataView
             |    ${aggs(i)}.resetAccumulator(acc$i);""".stripMargin
        }
      }.mkString("\n")

      if (needReset) {
        j"""$sig {
           |$reset
           |  }""".stripMargin
      } else {
        j"""$sig {
           |  }""".stripMargin
      }
    }

    val aggFuncCode = Seq(
      genSetAggregationResults,
      genAccumulate,
      genRetract,
      genCreateAccumulators,
      genSetForwardedFields,
      genSetConstantFlags,
      genCreateOutputRow,
      genMergeAccumulatorsPair,
      genResetAccumulator).mkString("\n")

    val generatedAggregationsClass = classOf[GeneratedAggregations].getCanonicalName
    var funcCode =
      j"""
         |public final class $funcName extends $generatedAggregationsClass {
         |
         |  ${reuseMemberCode()}
         |  $genMergeList
         |  public $funcName() throws Exception {
         |    ${reuseInitCode()}
         |  }
         |  ${reuseConstructorCode(funcName)}
         |
         |  public final void open(
         |    org.apache.flink.api.common.functions.RuntimeContext $contextTerm) throws Exception {
         |    ${reuseOpenCode()}
         |  }
         |
         |  $aggFuncCode
         |
         |  public final void cleanup() throws Exception {
         |    ${reuseCleanupCode()}
         |  }
         |
         |  public final void close() throws Exception {
         |    ${reuseCloseCode()}
         |  }
         |}
         """.stripMargin

    GeneratedAggregationsFunction(funcName, funcCode)
  }

}
