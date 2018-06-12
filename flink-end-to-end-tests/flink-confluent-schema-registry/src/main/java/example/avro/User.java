/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example.avro;

/**
 * Autogenerated by Avro
 * <p> DO NOT EDIT DIRECTLY </p>
 */

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;
/**
**/

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class User extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
	private static final long serialVersionUID = 3144724849436857910L;
	public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"favoriteNumber\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"favoriteColor\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"eventType\",\"type\":{\"type\":\"enum\",\"name\":\"EventType\",\"symbols\":[\"meeting\"]}}]}");

	public static org.apache.avro.Schema getClassSchema() {
		return SCHEMA$;
	}

	private static SpecificData MODEL$ = new SpecificData();

	private static final BinaryMessageEncoder<User> ENCODER =
		new BinaryMessageEncoder<User>(MODEL$, SCHEMA$);

	private static final BinaryMessageDecoder<User> DECODER =
		new BinaryMessageDecoder<User>(MODEL$, SCHEMA$);

	/**
	 * Return the BinaryMessageDecoder instance used by this class.
	 */
	public static BinaryMessageDecoder<User> getDecoder() {
		return DECODER;
	}

	/**
	 * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
	 * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
	 */
	public static BinaryMessageDecoder<User> createDecoder(SchemaStore resolver) {
		return new BinaryMessageDecoder<User>(MODEL$, SCHEMA$, resolver);
	}

	/** Serializes this User to a ByteBuffer. */
	public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
		return ENCODER.encode(this);
	}

	/** Deserializes a User from a ByteBuffer. */
	public static User fromByteBuffer(
		java.nio.ByteBuffer b) throws java.io.IOException {
		return DECODER.decode(b);
	}

	@Deprecated
	public CharSequence name;
	@Deprecated
	public CharSequence favoriteNumber;
	@Deprecated
	public CharSequence favoriteColor;
	@Deprecated
	public example.avro.EventType eventType;

	/**
	 * Default constructor.  Note that this does not initialize fields
	 * to their default values from the schema.  If that is desired then
	 * one should use <code>newBuilder()</code>.
	 */
	public User() {
	}

	/**
	 * All-args constructor.
	 * @param name The new value for name
	 * @param favoriteNumber The new value for favoriteNumber
	 * @param favoriteColor The new value for favoriteColor
	 * @param eventType The new value for eventType
	 */
	public User(CharSequence name, CharSequence favoriteNumber, CharSequence favoriteColor, example.avro.EventType eventType) {
		this.name = name;
		this.favoriteNumber = favoriteNumber;
		this.favoriteColor = favoriteColor;
		this.eventType = eventType;
	}

	public org.apache.avro.Schema getSchema() {
		return SCHEMA$;
	}

	// Used by DatumWriter.  Applications should not call.
	public Object get(int field$) {
		switch (field$) {
			case 0:
				return name;
			case 1:
				return favoriteNumber;
			case 2:
				return favoriteColor;
			case 3:
				return eventType;
			default:
				throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}

	// Used by DatumReader.  Applications should not call.
	@SuppressWarnings(value = "unchecked")
	public void put(int field$, Object value$) {
		switch (field$) {
			case 0:
				name = (CharSequence) value$;
				break;
			case 1:
				favoriteNumber = (CharSequence) value$;
				break;
			case 2:
				favoriteColor = (CharSequence) value$;
				break;
			case 3:
				eventType = (example.avro.EventType) value$;
				break;
			default:
				throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}

	/**
	 * Gets the value of the 'name' field.
	 * @return The value of the 'name' field.
	 */
	public CharSequence getName() {
		return name;
	}

	/**
	 * Sets the value of the 'name' field.
	 * @param value the value to set.
	 */
	public void setName(CharSequence value) {
		this.name = value;
	}

	/**
	 * Gets the value of the 'favoriteNumber' field.
	 * @return The value of the 'favoriteNumber' field.
	 */
	public CharSequence getFavoriteNumber() {
		return favoriteNumber;
	}

	/**
	 * Sets the value of the 'favoriteNumber' field.
	 * @param value the value to set.
	 */
	public void setFavoriteNumber(CharSequence value) {
		this.favoriteNumber = value;
	}

	/**
	 * Gets the value of the 'favoriteColor' field.
	 * @return The value of the 'favoriteColor' field.
	 */
	public CharSequence getFavoriteColor() {
		return favoriteColor;
	}

	/**
	 * Sets the value of the 'favoriteColor' field.
	 * @param value the value to set.
	 */
	public void setFavoriteColor(CharSequence value) {
		this.favoriteColor = value;
	}

	/**
	 * Gets the value of the 'eventType' field.
	 * @return The value of the 'eventType' field.
	 */
	public example.avro.EventType getEventType() {
		return eventType;
	}

	/**
	 * Sets the value of the 'eventType' field.
	 * @param value the value to set.
	 */
	public void setEventType(example.avro.EventType value) {
		this.eventType = value;
	}

	/**
	 * Creates a new User RecordBuilder.
	 * @return A new User RecordBuilder
	 */
	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Creates a new User RecordBuilder by copying an existing Builder.
	 * @param other The existing builder to copy.
	 * @return A new User RecordBuilder
	 */
	public static Builder newBuilder(Builder other) {
		return new Builder(other);
	}

	/**
	 * Creates a new User RecordBuilder by copying an existing User instance.
	 * @param other The existing instance to copy.
	 * @return A new User RecordBuilder
	 */
	public static Builder newBuilder(User other) {
		return new Builder(other);
	}

	/**
	 * RecordBuilder for User instances.
	 */
	public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<User>
		implements org.apache.avro.data.RecordBuilder<User> {

		private CharSequence name;
		private CharSequence favoriteNumber;
		private CharSequence favoriteColor;
		private example.avro.EventType eventType;

		/** Creates a new Builder. */
		private Builder() {
			super(SCHEMA$);
		}

		/**
		 * Creates a Builder by copying an existing Builder.
		 * @param other The existing Builder to copy.
		 */
		private Builder(Builder other) {
			super(other);
			if (isValidValue(fields()[0], other.name)) {
				this.name = data().deepCopy(fields()[0].schema(), other.name);
				fieldSetFlags()[0] = true;
			}
			if (isValidValue(fields()[1], other.favoriteNumber)) {
				this.favoriteNumber = data().deepCopy(fields()[1].schema(), other.favoriteNumber);
				fieldSetFlags()[1] = true;
			}
			if (isValidValue(fields()[2], other.favoriteColor)) {
				this.favoriteColor = data().deepCopy(fields()[2].schema(), other.favoriteColor);
				fieldSetFlags()[2] = true;
			}
			if (isValidValue(fields()[3], other.eventType)) {
				this.eventType = data().deepCopy(fields()[3].schema(), other.eventType);
				fieldSetFlags()[3] = true;
			}
		}

		/**
		 * Creates a Builder by copying an existing User instance.
		 * @param other The existing instance to copy.
		 */
		private Builder(User other) {
			super(SCHEMA$);
			if (isValidValue(fields()[0], other.name)) {
				this.name = data().deepCopy(fields()[0].schema(), other.name);
				fieldSetFlags()[0] = true;
			}
			if (isValidValue(fields()[1], other.favoriteNumber)) {
				this.favoriteNumber = data().deepCopy(fields()[1].schema(), other.favoriteNumber);
				fieldSetFlags()[1] = true;
			}
			if (isValidValue(fields()[2], other.favoriteColor)) {
				this.favoriteColor = data().deepCopy(fields()[2].schema(), other.favoriteColor);
				fieldSetFlags()[2] = true;
			}
			if (isValidValue(fields()[3], other.eventType)) {
				this.eventType = data().deepCopy(fields()[3].schema(), other.eventType);
				fieldSetFlags()[3] = true;
			}
		}

		/**
		 * Gets the value of the 'name' field.
		 * @return The value.
		 */
		public CharSequence getName() {
			return name;
		}

		/**
		 * Sets the value of the 'name' field.
		 * @param value The value of 'name'.
		 * @return This builder.
		 */
		public Builder setName(CharSequence value) {
			validate(fields()[0], value);
			this.name = value;
			fieldSetFlags()[0] = true;
			return this;
		}

		/**
		 * Checks whether the 'name' field has been set.
		 * @return True if the 'name' field has been set, false otherwise.
		 */
		public boolean hasName() {
			return fieldSetFlags()[0];
		}


		/**
		 * Clears the value of the 'name' field.
		 * @return This builder.
		 */
		public Builder clearName() {
			name = null;
			fieldSetFlags()[0] = false;
			return this;
		}

		/**
		 * Gets the value of the 'favoriteNumber' field.
		 * @return The value.
		 */
		public CharSequence getFavoriteNumber() {
			return favoriteNumber;
		}

		/**
		 * Sets the value of the 'favoriteNumber' field.
		 * @param value The value of 'favoriteNumber'.
		 * @return This builder.
		 */
		public Builder setFavoriteNumber(CharSequence value) {
			validate(fields()[1], value);
			this.favoriteNumber = value;
			fieldSetFlags()[1] = true;
			return this;
		}

		/**
		 * Checks whether the 'favoriteNumber' field has been set.
		 * @return True if the 'favoriteNumber' field has been set, false otherwise.
		 */
		public boolean hasFavoriteNumber() {
			return fieldSetFlags()[1];
		}


		/**
		 * Clears the value of the 'favoriteNumber' field.
		 * @return This builder.
		 */
		public Builder clearFavoriteNumber() {
			favoriteNumber = null;
			fieldSetFlags()[1] = false;
			return this;
		}

		/**
		 * Gets the value of the 'favoriteColor' field.
		 * @return The value.
		 */
		public CharSequence getFavoriteColor() {
			return favoriteColor;
		}

		/**
		 * Sets the value of the 'favoriteColor' field.
		 * @param value The value of 'favoriteColor'.
		 * @return This builder.
		 */
		public Builder setFavoriteColor(CharSequence value) {
			validate(fields()[2], value);
			this.favoriteColor = value;
			fieldSetFlags()[2] = true;
			return this;
		}

		/**
		 * Checks whether the 'favoriteColor' field has been set.
		 * @return True if the 'favoriteColor' field has been set, false otherwise.
		 */
		public boolean hasFavoriteColor() {
			return fieldSetFlags()[2];
		}


		/**
		 * Clears the value of the 'favoriteColor' field.
		 * @return This builder.
		 */
		public Builder clearFavoriteColor() {
			favoriteColor = null;
			fieldSetFlags()[2] = false;
			return this;
		}

		/**
		 * Gets the value of the 'eventType' field.
		 * @return The value.
		 */
		public example.avro.EventType getEventType() {
			return eventType;
		}

		/**
		 * Sets the value of the 'eventType' field.
		 * @param value The value of 'eventType'.
		 * @return This builder.
		 */
		public Builder setEventType(example.avro.EventType value) {
			validate(fields()[3], value);
			this.eventType = value;
			fieldSetFlags()[3] = true;
			return this;
		}

		/**
		 * Checks whether the 'eventType' field has been set.
		 * @return True if the 'eventType' field has been set, false otherwise.
		 */
		public boolean hasEventType() {
			return fieldSetFlags()[3];
		}


		/**
		 * Clears the value of the 'eventType' field.
		 * @return This builder.
		 */
		public Builder clearEventType() {
			eventType = null;
			fieldSetFlags()[3] = false;
			return this;
		}

		@Override
		@SuppressWarnings("unchecked")
		public User build() {
			try {
				User record = new User();
				record.name = fieldSetFlags()[0] ? this.name : (CharSequence) defaultValue(fields()[0]);
				record.favoriteNumber = fieldSetFlags()[1] ? this.favoriteNumber : (CharSequence) defaultValue(fields()[1]);
				record.favoriteColor = fieldSetFlags()[2] ? this.favoriteColor : (CharSequence) defaultValue(fields()[2]);
				record.eventType = fieldSetFlags()[3] ? this.eventType : (example.avro.EventType) defaultValue(fields()[3]);
				return record;
			} catch (Exception e) {
				throw new org.apache.avro.AvroRuntimeException(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static final org.apache.avro.io.DatumWriter<User>
		WRITER$ = (org.apache.avro.io.DatumWriter<User>) MODEL$.createDatumWriter(SCHEMA$);

	@Override
	public void writeExternal(java.io.ObjectOutput out)
		throws java.io.IOException {
		WRITER$.write(this, SpecificData.getEncoder(out));
	}

	@SuppressWarnings("unchecked")
	private static final org.apache.avro.io.DatumReader<User>
		READER$ = (org.apache.avro.io.DatumReader<User>) MODEL$.createDatumReader(SCHEMA$);

	@Override
	public void readExternal(java.io.ObjectInput in)
		throws java.io.IOException {
		READER$.read(this, SpecificData.getDecoder(in));
	}

}
