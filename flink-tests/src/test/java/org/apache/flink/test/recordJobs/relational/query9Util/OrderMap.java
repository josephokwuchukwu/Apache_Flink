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


package org.apache.flink.test.recordJobs.relational.query9Util;

import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.test.recordJobs.util.Tuple;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class OrderMap extends MapFunction {
	
	private final Tuple inputTuple = new Tuple();
	
	/**
	 * Project "orders"
	 * 
	 * Output Schema:
	 *  Key: orderkey
	 *  Value: year (from date)
	 *
	 */
	@Override
	public void map(Record record, Collector<Record> out) throws Exception {
		Tuple inputTuple = record.getField(1, this.inputTuple);
		
		int year = Integer.parseInt(inputTuple.getStringValueAt(4).substring(0, 4));
		record.setField(1, new IntValue(year));
		out.collect(record);
	}

}
