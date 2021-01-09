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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializer;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TraversableOnceException;

import org.junit.Before;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Test for the key grouped iterator, which advances in windows containing the same key and provides
 * a sub-iterator over the records with the same key.
 */
public class ReusingKeyGroupedIteratorTest {

    private MutableObjectIterator<Record> sourceIter; // the iterator that provides the input

    private ReusingKeyGroupedIterator<Record>
            psi; // the grouping iterator, progressing in key steps

    @Before
    public void setup() {
        final ArrayList<IntStringPair> source = new ArrayList<IntStringPair>();

        // add elements to the source
        source.add(new IntStringPair(new IntValue(1), new StringValue("A")));
        source.add(new IntStringPair(new IntValue(2), new StringValue("B")));
        source.add(new IntStringPair(new IntValue(3), new StringValue("C")));
        source.add(new IntStringPair(new IntValue(3), new StringValue("D")));
        source.add(new IntStringPair(new IntValue(4), new StringValue("E")));
        source.add(new IntStringPair(new IntValue(4), new StringValue("F")));
        source.add(new IntStringPair(new IntValue(4), new StringValue("G")));
        source.add(new IntStringPair(new IntValue(5), new StringValue("H")));
        source.add(new IntStringPair(new IntValue(5), new StringValue("I")));
        source.add(new IntStringPair(new IntValue(5), new StringValue("J")));
        source.add(new IntStringPair(new IntValue(5), new StringValue("K")));
        source.add(new IntStringPair(new IntValue(5), new StringValue("L")));

        this.sourceIter =
                new MutableObjectIterator<Record>() {
                    final Iterator<IntStringPair> it = source.iterator();

                    @Override
                    public Record next(Record reuse) throws IOException {
                        if (it.hasNext()) {
                            IntStringPair pair = it.next();
                            reuse.setField(0, pair.getInteger());
                            reuse.setField(1, pair.getString());
                            return reuse;
                        } else {
                            return null;
                        }
                    }

                    @Override
                    public Record next() throws IOException {
                        if (it.hasNext()) {
                            IntStringPair pair = it.next();
                            Record result = new Record(2);
                            result.setField(0, pair.getInteger());
                            result.setField(1, pair.getString());
                            return result;
                        } else {
                            return null;
                        }
                    }
                };

        final RecordSerializer serializer = RecordSerializer.get();
        @SuppressWarnings("unchecked")
        final RecordComparator comparator =
                new RecordComparator(new int[] {0}, new Class[] {IntValue.class});

        this.psi = new ReusingKeyGroupedIterator<Record>(this.sourceIter, serializer, comparator);
    }

    @Test
    public void testNextKeyOnly() throws Exception {
        try {
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(1))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    1,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");

            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(2))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    2,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");

            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(3))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    3,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");

            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(4))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    4,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");

            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(5))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    5,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");

            Assertions.assertFalse(
                    this.psi.nextKey(), "KeyGroupedIterator must not have another key.");
            Assertions.assertNull(
                    this.psi.getValues(), "KeyGroupedIterator must not have another value.");

            Assertions.assertFalse(
                    this.psi.nextKey(), "KeyGroupedIterator must not have another key.");
            Assertions.assertFalse(
                    this.psi.nextKey(), "KeyGroupedIterator must not have another key.");
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("The test encountered an unexpected exception.");
        }
    }

    @Test
    public void testFullIterationThroughAllValues() throws IOException {
        try {
            // Key 1, Value A
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(hasIterator(this.psi.getValues()));
            Assertions.assertFalse(hasIterator(this.psi.getValues()));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(1))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    1,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("A"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertFalse(
                    this.psi.getValues().hasNext(),
                    "KeyGroupedIterator must not have another value.");

            // Key 2, Value B
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(hasIterator(this.psi.getValues()));
            Assertions.assertFalse(hasIterator(this.psi.getValues()));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(2))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    2,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("B"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertFalse(
                    this.psi.getValues().hasNext(),
                    "KeyGroupedIterator must not have another value.");

            // Key 3, Values C, D
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(hasIterator(this.psi.getValues()));
            Assertions.assertFalse(hasIterator(this.psi.getValues()));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(3))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    3,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("C"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(3))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    3,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("D"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(3))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    3,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            try {
                this.psi.getValues().next();
                Assertions.fail(
                        "A new KeyGroupedIterator must not have any value available and hence throw an exception on next().");
            } catch (NoSuchElementException nseex) {
            }
            Assertions.assertFalse(
                    this.psi.getValues().hasNext(),
                    "KeyGroupedIterator must not have another value.");
            try {
                this.psi.getValues().next();
                Assertions.fail(
                        "A new KeyGroupedIterator must not have any value available and hence throw an exception on next().");
            } catch (NoSuchElementException nseex) {
            }
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(3))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    3,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");

            // Key 4, Values E, F, G
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(hasIterator(this.psi.getValues()));
            Assertions.assertFalse(hasIterator(this.psi.getValues()));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(4))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    4,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("E"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(4))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    4,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("F"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(4))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    4,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("G"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(4))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    4,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertFalse(
                    this.psi.getValues().hasNext(),
                    "KeyGroupedIterator must not have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(4))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    4,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");

            // Key 5, Values H, I, J, K, L
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(hasIterator(this.psi.getValues()));
            Assertions.assertFalse(hasIterator(this.psi.getValues()));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(5))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    5,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("H"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(5))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    5,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("I"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(5))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    5,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("J"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(5))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    5,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("K"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(5))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    5,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("L"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(5))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    5,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            try {
                this.psi.getValues().next();
                Assertions.fail(
                        "A new KeyGroupedIterator must not have any value available and hence throw an exception on next().");
            } catch (NoSuchElementException nseex) {
            }
            Assertions.assertFalse(
                    this.psi.getValues().hasNext(),
                    "KeyGroupedIterator must not have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(5))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    5,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            try {
                this.psi.getValues().next();
                Assertions.fail(
                        "A new KeyGroupedIterator must not have any value available and hence throw an exception on next().");
            } catch (NoSuchElementException nseex) {
            }

            Assertions.assertFalse(
                    this.psi.nextKey(), "KeyGroupedIterator must not have another key.");
            Assertions.assertFalse(
                    this.psi.nextKey(), "KeyGroupedIterator must not have another key.");
            Assertions.assertNull(this.psi.getValues());
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("The test encountered an unexpected exception.");
        }
    }

    @Test
    public void testMixedProgress() throws Exception {
        try {
            // Progression only via nextKey() and hasNext() - Key 1, Value A
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(hasIterator(this.psi.getValues()));
            Assertions.assertFalse(hasIterator(this.psi.getValues()));

            // Progression only through nextKey() - Key 2, Value B
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(hasIterator(this.psi.getValues()));
            Assertions.assertFalse(hasIterator(this.psi.getValues()));

            // Progression first though haNext() and next(), then through hasNext() - Key 3, Values
            // C, D
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(hasIterator(this.psi.getValues()));
            Assertions.assertFalse(hasIterator(this.psi.getValues()));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(3))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    3,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("C"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(3))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    3,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");

            // Progression first via next() only, then hasNext() only Key 4, Values E, F, G
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertTrue(hasIterator(this.psi.getValues()));
            Assertions.assertFalse(hasIterator(this.psi.getValues()));
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("E"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");

            // Key 5, Values H, I, J, K, L
            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("H"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertTrue(
                    this.psi
                            .getComparatorWithCurrentReference()
                            .equalToReference(new Record(new IntValue(5))),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    5,
                    this.psi.getCurrent().getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("I"),
                    this.psi.getValues().next().getField(1, StringValue.class));
            Assertions.assertTrue(hasIterator(this.psi.getValues()));
            Assertions.assertFalse(hasIterator(this.psi.getValues()));
            Assertions.assertTrue(
                    this.psi.getValues().hasNext(), "KeyGroupedIterator must have another value.");

            // end
            Assertions.assertFalse(
                    this.psi.nextKey(), "KeyGroupedIterator must not have another key.");
            Assertions.assertFalse(
                    this.psi.nextKey(), "KeyGroupedIterator must not have another key.");
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("The test encountered an unexpected exception.");
        }
    }

    @Test
    public void testHasNextDoesNotOverweiteCurrentRecord() throws Exception {
        try {
            Iterator<Record> valsIter = null;
            Record rec = null;

            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            valsIter = this.psi.getValues();
            Assertions.assertNotNull(valsIter, "Returned Iterator must not be null");
            Assertions.assertTrue(
                    valsIter.hasNext(),
                    "KeyGroupedIterator's value iterator must have another value.");
            rec = valsIter.next();
            Assertions.assertEquals(
                    1,
                    rec.getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("A"),
                    rec.getField(1, StringValue.class));
            Assertions.assertFalse(
                    valsIter.hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertEquals(
                    1,
                    rec.getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("A"),
                    rec.getField(1, StringValue.class));
            Assertions.assertFalse(
                    valsIter.hasNext(),
                    "KeyGroupedIterator's value iterator must not have another value.");

            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            valsIter = this.psi.getValues();
            Assertions.assertNotNull(valsIter, "Returned Iterator must not be null");
            Assertions.assertTrue(
                    valsIter.hasNext(),
                    "KeyGroupedIterator's value iterator must have another value.");
            rec = valsIter.next();
            Assertions.assertEquals(
                    2,
                    rec.getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("B"),
                    rec.getField(1, StringValue.class));
            Assertions.assertFalse(
                    valsIter.hasNext(), "KeyGroupedIterator must have another value.");
            Assertions.assertEquals(
                    2,
                    rec.getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("B"),
                    rec.getField(1, StringValue.class));
            Assertions.assertFalse(
                    valsIter.hasNext(),
                    "KeyGroupedIterator's value iterator must not have another value.");

            Assertions.assertTrue(this.psi.nextKey(), "KeyGroupedIterator must have another key.");
            valsIter = this.psi.getValues();
            Assertions.assertNotNull(valsIter, "Returned Iterator must not be null");
            Assertions.assertTrue(
                    valsIter.hasNext(),
                    "KeyGroupedIterator's value iterator must have another value.");
            rec = valsIter.next();
            Assertions.assertEquals(
                    3,
                    rec.getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("C"),
                    rec.getField(1, StringValue.class));
            Assertions.assertTrue(
                    valsIter.hasNext(),
                    "KeyGroupedIterator's value iterator must have another value.");
            Assertions.assertEquals(
                    3,
                    rec.getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("C"),
                    rec.getField(1, StringValue.class));
            rec = valsIter.next();
            Assertions.assertEquals(
                    3,
                    rec.getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("D"),
                    rec.getField(1, StringValue.class));
            Assertions.assertFalse(
                    valsIter.hasNext(),
                    "KeyGroupedIterator's value iterator must have another value.");
            Assertions.assertEquals(
                    3,
                    rec.getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("D"),
                    rec.getField(1, StringValue.class));
            Assertions.assertFalse(
                    valsIter.hasNext(),
                    "KeyGroupedIterator's value iterator must have another value.");
            Assertions.assertEquals(
                    3,
                    rec.getField(0, IntValue.class).getValue(),
                    "KeyGroupedIterator returned a wrong key.");
            Assertions.assertEquals(
                    "KeyGroupedIterator returned a wrong value.",
                    new StringValue("D"),
                    rec.getField(1, StringValue.class));
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("The test encountered an unexpected exception.");
        }
    }

    private static final class IntStringPair {
        private final IntValue integer;
        private final StringValue string;

        IntStringPair(IntValue integer, StringValue string) {
            this.integer = integer;
            this.string = string;
        }

        public IntValue getInteger() {
            return integer;
        }

        public StringValue getString() {
            return string;
        }
    }

    public boolean hasIterator(Iterable<?> iterable) {
        try {
            iterable.iterator();
            return true;
        } catch (TraversableOnceException e) {
            return false;
        }
    }
}
