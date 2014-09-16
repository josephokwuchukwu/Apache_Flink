/**
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


package org.apache.flink.compiler.dataproperties;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;

/**
 * This class represents local properties of the data. A local property is a property that exists
 * within the data of a single partition.
 */
public class LocalProperties implements Cloneable {
	
	public static final LocalProperties EMPTY = new LocalProperties();
	
	// --------------------------------------------------------------------------------------------
	
	private Ordering ordering;			// order inside a partition, null if not ordered

	private FieldList groupedFields;		// fields by which the stream is grouped. null if not grouped.
	
	private Set<FieldSet> uniqueFields;		// fields whose value combination is unique in the stream

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Default constructor for trivial local properties. No order, no grouping, no uniqueness.
	 */
	public LocalProperties() {}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the key order.
	 * 
	 * @return The key order, or <code>null</code> if nothing is ordered.
	 */
	public Ordering getOrdering() {
		return ordering;
	}
	
	/**
	 * Gets the grouped fields.
	 * 
	 * @return The grouped fields, or <code>null</code> if nothing is grouped.
	 */
	public FieldList getGroupedFields() {
		return this.groupedFields;
	}

	/**
	 * Gets the fields whose combination is unique within the data set.
	 * 
	 * @return The unique field combination, or <code>null</code> if nothing is unique.
	 */
	public Set<FieldSet> getUniqueFields() {
		return this.uniqueFields;
	}
	
	/**
	 * Checks whether the given set of fields is unique, as specified in these local properties.
	 * 
	 * @param set The set to check.
	 * @return True, if the given column combination is unique, false if not.
	 */
	public boolean areFieldsUnique(FieldSet set) {
		return this.uniqueFields != null && this.uniqueFields.contains(set);
	}
	
	/**
	 * Adds a combination of fields that are unique in these data properties.
	 * 
	 * @param uniqueFields The fields that are unique in these data properties.
	 */
	public LocalProperties addUniqueFields(FieldSet uniqueFields) {
		LocalProperties copy = clone();
		
		if (copy.uniqueFields == null) {
			copy.uniqueFields = new HashSet<FieldSet>();
		}
		copy.uniqueFields.add(uniqueFields);
		return copy;
	}
	
	public LocalProperties clearUniqueFieldSets() {
		if (this.uniqueFields == null || this.uniqueFields.isEmpty()) {
			return this;
		} else {
			LocalProperties copy = new LocalProperties();
			copy.ordering = this.ordering;
			copy.groupedFields = this.groupedFields;
			return copy;
		}
	}
	
	/**
	 * Checks, if the properties in this object are trivial, i.e. only standard values.
	 */
	public boolean isTrivial() {
		return ordering == null && this.groupedFields == null && this.uniqueFields == null;
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Filters these properties by what can be preserved through a user function's constant fields set.
	 *
	 * @param props The optimizer node that potentially modifies the properties.
	 * @param input The input of the node which is relevant.
	 *
	 * @return The filtered LocalProperties
	 */
	public LocalProperties filterBySemanticProperties(SemanticProperties props, int input) {
		// check, whether the local order is preserved
		Ordering no = this.ordering;
		FieldList ngf = this.groupedFields;
		Set<FieldSet> nuf = this.uniqueFields;
		FieldList forwardList = null;

		if (props == null) {
			return new LocalProperties();
		}

		if (this.ordering != null) {
			no = new Ordering();
			final FieldList involvedIndexes = this.ordering.getInvolvedIndexes();
			for (int i = 0; i < involvedIndexes.size(); i++) {
				forwardList = props.getForwardFields(input, involvedIndexes.get(i)) == null ? null : props.getForwardFields(input, involvedIndexes.get(i)).toFieldList();

				if (forwardList == null) {
					no = null;
					ngf = null;
					/*if (i == 0) {
						no = null;
						ngf = null;
					} else {
						no = this.ordering.createNewOrderingUpToIndex(i);
						ngf = no.getInvolvedIndexes();
					}*/
					break;
				} else {
					no.appendOrdering(forwardList.get(0), this.ordering.getType(i), this.ordering.getOrder(i));
					ngf = no.getInvolvedIndexes();
				}
			}
		}
		else if (this.groupedFields != null) {
			// check, whether the local key grouping is preserved
			for (Integer index : this.groupedFields) {
				forwardList = props.getForwardFields(input, index) == null ? null : props.getForwardFields(input, index).toFieldList();
				if (forwardList == null) {
					ngf = null;
					break;
				} else if (!forwardList.contains(index)) {
					FieldList grouped = new FieldList();
					for (Integer value : ngf.toFieldList()) {
						if (value.intValue() == index) {
							grouped = grouped.addFields(forwardList);
						} else {
							grouped = grouped.addField(value);
						}
					}
					ngf = grouped;
				}
			}
		}

		if (this.uniqueFields != null) {
			HashSet<FieldSet> newSet = new HashSet<FieldSet>();
			newSet.addAll(this.uniqueFields);
			for (Iterator<FieldSet> combos = this.uniqueFields.iterator(); combos.hasNext(); ){
				FieldSet current = combos.next();
				FieldSet nfs = new FieldSet();
				for (Integer field : current) {
					if (props.getForwardFields(input, field) == null) {
						newSet.remove(current);
						nfs = null;
						break;
					} else {
						nfs = nfs.addFields(props.getForwardFields(input, field));
					}
				}
				if (nfs != null) {
					newSet.remove(current);
					newSet.add(nfs);
				}
			}

			nuf = newSet.isEmpty() ? null : newSet;
		}

		if (no == this.ordering && ngf == this.groupedFields && nuf == this.uniqueFields) {
			return this;
		} else {
			LocalProperties lp = new LocalProperties();
			lp.ordering = no;
			lp.groupedFields = ngf;
			lp.uniqueFields = nuf;
			return lp;
		}
	}
	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.ordering == null ? 0 : this.ordering.hashCode());
		result = prime * result + (this.groupedFields == null ? 0 : this.groupedFields.hashCode());
		result = prime * result + (this.uniqueFields == null ? 0 : this.uniqueFields.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof LocalProperties) {
			final LocalProperties other = (LocalProperties) obj;
			return (ordering == other.getOrdering() || (ordering != null && ordering.equals(other.getOrdering()))) &&
				(groupedFields == other.getGroupedFields() || (groupedFields != null && groupedFields.equals(other.getGroupedFields()))) &&
				(uniqueFields == other.getUniqueFields() || (uniqueFields != null && uniqueFields.equals(other.getUniqueFields())));
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "LocalProperties [ordering=" + this.ordering + ", grouped=" + this.groupedFields
			+ ", unique=" + this.uniqueFields + "]";
	}

	@Override
	public LocalProperties clone() {
		LocalProperties copy = new LocalProperties();
		copy.ordering = this.ordering;
		copy.groupedFields = this.groupedFields;
		copy.uniqueFields = (this.uniqueFields == null ? null : new HashSet<FieldSet>(this.uniqueFields));
		return copy;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static LocalProperties combine(LocalProperties lp1, LocalProperties lp2) {
		if (lp1.ordering != null) {
			return lp1;
		} else if (lp2.ordering != null) {
			return lp2;
		} else if (lp1.groupedFields != null) {
			return lp1;
		} else if (lp2.groupedFields != null) {
			return lp2;
		} else if (lp1.uniqueFields != null && !lp1.uniqueFields.isEmpty()) {
			return lp1;
		} else if (lp2.uniqueFields != null && !lp2.uniqueFields.isEmpty()) {
			return lp2;
		} else {
			return lp1;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static LocalProperties forOrdering(Ordering o) {
		LocalProperties props = new LocalProperties();
		props.ordering = o;
		props.groupedFields = o.getInvolvedIndexes();
		return props;
	}
	
	public static LocalProperties forGrouping(FieldList groupedFields) {
		LocalProperties props = new LocalProperties();
		props.groupedFields = groupedFields;
		return props;
	}
}
