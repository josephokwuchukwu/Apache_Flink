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

package org.apache.flink.hadoopcompatibility.mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.WritableTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoopcompatibility.mapred.utils.HadoopConfiguration;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reporter;

/**
 * The wrapper for a Hadoop Mapper (mapred API). Parses a Hadoop JobConf object and initialises all operations related
 * mappers.
 */
public final class HadoopMapFunction<KEYIN extends WritableComparable<?>, VALUEIN extends Writable,
										KEYOUT extends WritableComparable<?>, VALUEOUT extends Writable> 
					extends RichFlatMapFunction<Tuple2<KEYIN,VALUEIN>, Tuple2<KEYOUT,VALUEOUT>> 
					implements ResultTypeQueryable<Tuple2<KEYOUT,VALUEOUT>>, Serializable {

	private static final long serialVersionUID = 1L;

	private transient Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> mapper;
	private transient JobConf jobConf;

	private transient Class<KEYOUT> keyOutClass;
	private transient Class<VALUEOUT> valOutClass;
	
	private transient HadoopOutputCollector<KEYOUT,VALUEOUT> outputCollector;
	private transient Reporter reporter;
	
	public HadoopMapFunction(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopMapper, 
								Class<KEYOUT> keyOutClass, Class<VALUEOUT> valOutClass) {
		
		this(hadoopMapper, keyOutClass, valOutClass, new JobConf());
	}
	
	public HadoopMapFunction(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> hadoopMapper, 
								Class<KEYOUT> keyOutClass, Class<VALUEOUT> valOutClass, JobConf conf) {
		this.mapper = hadoopMapper;
		this.keyOutClass = keyOutClass;
		this.valOutClass = valOutClass;
		
		this.jobConf = conf;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.mapper.configure(jobConf);
		
		this.reporter = new HadoopDummyReporter();
		this.outputCollector = new HadoopOutputCollector<KEYOUT, VALUEOUT>();
	}

	/**
	 * Wrap a hadoop map() function call and use a Flink collector to collect the result values.
	 * @param value The input value.
	 * @param out The collector for emitting result values.
	 *
	 * @throws Exception
	 */
	@Override
	public void flatMap(final Tuple2<KEYIN,VALUEIN> value, final Collector<Tuple2<KEYOUT,VALUEOUT>> out) 
			throws Exception {
		outputCollector.setFlinkCollector(out);
		mapper.map(value.f0, value.f1, outputCollector, reporter);
	}

	@Override
	public TypeInformation<Tuple2<KEYOUT,VALUEOUT>> getProducedType() {
		final WritableTypeInfo<KEYOUT> keyTypeInfo = new WritableTypeInfo<KEYOUT>(keyOutClass);
		final WritableTypeInfo<VALUEOUT> valueTypleInfo = new WritableTypeInfo<VALUEOUT>(valOutClass);
		return new TupleTypeInfo<Tuple2<KEYOUT,VALUEOUT>>(keyTypeInfo, valueTypleInfo);
	}
	
	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(final ObjectOutputStream out) throws IOException {
		out.writeObject(mapper.getClass());
		HadoopConfiguration.writeHadoopJobConf(jobConf, out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
		Class<Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>> mapperClass = 
				(Class<Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>>)in.readObject();
		mapper = InstantiationUtil.instantiate(mapperClass);
		
		jobConf = new JobConf();
		jobConf.readFields(in);
	}

}
