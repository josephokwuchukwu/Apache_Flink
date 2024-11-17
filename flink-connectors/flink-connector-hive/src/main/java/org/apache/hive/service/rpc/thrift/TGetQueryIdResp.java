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

package org.apache.hive.service.rpc.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@org.apache.hadoop.hive.common.classification.InterfaceAudience.Public
@org.apache.hadoop.hive.common.classification.InterfaceStability.Stable
public class TGetQueryIdResp
        implements org.apache.thrift.TBase<TGetQueryIdResp, TGetQueryIdResp._Fields>,
                java.io.Serializable,
                Cloneable,
                Comparable<TGetQueryIdResp> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC =
            new org.apache.thrift.protocol.TStruct("TGetQueryIdResp");

    private static final org.apache.thrift.protocol.TField QUERY_ID_FIELD_DESC =
            new org.apache.thrift.protocol.TField(
                    "queryId", org.apache.thrift.protocol.TType.STRING, (short) 1);

    private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY =
            new TGetQueryIdRespStandardSchemeFactory();
    private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY =
            new TGetQueryIdRespTupleSchemeFactory();

    private java.lang.String queryId; // required

    /**
     * The set of fields this struct contains, along with convenience methods for finding and
     * manipulating them.
     */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        QUERY_ID((short) 1, "queryId");

        private static final java.util.Map<java.lang.String, _Fields> byName =
                new java.util.HashMap<java.lang.String, _Fields>();

        static {
            for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
                byName.put(field.getFieldName(), field);
            }
        }

        /** Find the _Fields constant that matches fieldId, or null if its not found. */
        public static _Fields findByThriftId(int fieldId) {
            switch (fieldId) {
                case 1: // QUERY_ID
                    return QUERY_ID;
                default:
                    return null;
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, throwing an exception if it is not found.
         */
        public static _Fields findByThriftIdOrThrow(int fieldId) {
            _Fields fields = findByThriftId(fieldId);
            if (fields == null)
                throw new java.lang.IllegalArgumentException(
                        "Field " + fieldId + " doesn't exist!");
            return fields;
        }

        /** Find the _Fields constant that matches name, or null if its not found. */
        public static _Fields findByName(java.lang.String name) {
            return byName.get(name);
        }

        private final short _thriftId;
        private final java.lang.String _fieldName;

        _Fields(short thriftId, java.lang.String fieldName) {
            _thriftId = thriftId;
            _fieldName = fieldName;
        }

        public short getThriftFieldId() {
            return _thriftId;
        }

        public java.lang.String getFieldName() {
            return _fieldName;
        }
    }

    // isset id assignments
    public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData>
            metaDataMap;

    static {
        java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap =
                new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
                        _Fields.class);
        tmpMap.put(
                _Fields.QUERY_ID,
                new org.apache.thrift.meta_data.FieldMetaData(
                        "queryId",
                        org.apache.thrift.TFieldRequirementType.REQUIRED,
                        new org.apache.thrift.meta_data.FieldValueMetaData(
                                org.apache.thrift.protocol.TType.STRING)));
        metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
                TGetQueryIdResp.class, metaDataMap);
    }

    public TGetQueryIdResp() {}

    public TGetQueryIdResp(java.lang.String queryId) {
        this();
        this.queryId = queryId;
    }

    /** Performs a deep copy on <i>other</i>. */
    public TGetQueryIdResp(TGetQueryIdResp other) {
        if (other.isSetQueryId()) {
            this.queryId = other.queryId;
        }
    }

    public TGetQueryIdResp deepCopy() {
        return new TGetQueryIdResp(this);
    }

    @Override
    public void clear() {
        this.queryId = null;
    }

    public java.lang.String getQueryId() {
        return this.queryId;
    }

    public void setQueryId(java.lang.String queryId) {
        this.queryId = queryId;
    }

    public void unsetQueryId() {
        this.queryId = null;
    }

    /** Returns true if field queryId is set (has been assigned a value) and false otherwise */
    public boolean isSetQueryId() {
        return this.queryId != null;
    }

    public void setQueryIdIsSet(boolean value) {
        if (!value) {
            this.queryId = null;
        }
    }

    public void setFieldValue(_Fields field, java.lang.Object value) {
        switch (field) {
            case QUERY_ID:
                if (value == null) {
                    unsetQueryId();
                } else {
                    setQueryId((java.lang.String) value);
                }
                break;
        }
    }

    public java.lang.Object getFieldValue(_Fields field) {
        switch (field) {
            case QUERY_ID:
                return getQueryId();
        }
        throw new java.lang.IllegalStateException();
    }

    /**
     * Returns true if field corresponding to fieldID is set (has been assigned a value) and false
     * otherwise
     */
    public boolean isSet(_Fields field) {
        if (field == null) {
            throw new java.lang.IllegalArgumentException();
        }

        switch (field) {
            case QUERY_ID:
                return isSetQueryId();
        }
        throw new java.lang.IllegalStateException();
    }

    @Override
    public boolean equals(java.lang.Object that) {
        if (that == null) return false;
        if (that instanceof TGetQueryIdResp) return this.equals((TGetQueryIdResp) that);
        return false;
    }

    public boolean equals(TGetQueryIdResp that) {
        if (that == null) return false;
        if (this == that) return true;

        boolean this_present_queryId = true && this.isSetQueryId();
        boolean that_present_queryId = true && that.isSetQueryId();
        if (this_present_queryId || that_present_queryId) {
            if (!(this_present_queryId && that_present_queryId)) return false;
            if (!this.queryId.equals(that.queryId)) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 1;

        hashCode = hashCode * 8191 + ((isSetQueryId()) ? 131071 : 524287);
        if (isSetQueryId()) hashCode = hashCode * 8191 + queryId.hashCode();

        return hashCode;
    }

    @Override
    public int compareTo(TGetQueryIdResp other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = java.lang.Boolean.valueOf(isSetQueryId()).compareTo(other.isSetQueryId());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetQueryId()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.queryId, other.queryId);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

    public _Fields fieldForId(int fieldId) {
        return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot)
            throws org.apache.thrift.TException {
        scheme(iprot).read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot)
            throws org.apache.thrift.TException {
        scheme(oprot).write(oprot, this);
    }

    @Override
    public java.lang.String toString() {
        java.lang.StringBuilder sb = new java.lang.StringBuilder("TGetQueryIdResp(");
        boolean first = true;

        sb.append("queryId:");
        if (this.queryId == null) {
            sb.append("null");
        } else {
            sb.append(this.queryId);
        }
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        if (!isSetQueryId()) {
            throw new org.apache.thrift.protocol.TProtocolException(
                    "Required field 'queryId' is unset! Struct:" + toString());
        }

        // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        try {
            write(
                    new org.apache.thrift.protocol.TCompactProtocol(
                            new org.apache.thrift.transport.TIOStreamTransport(out)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws java.io.IOException, java.lang.ClassNotFoundException {
        try {
            read(
                    new org.apache.thrift.protocol.TCompactProtocol(
                            new org.apache.thrift.transport.TIOStreamTransport(in)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private static class TGetQueryIdRespStandardSchemeFactory
            implements org.apache.thrift.scheme.SchemeFactory {
        public TGetQueryIdRespStandardScheme getScheme() {
            return new TGetQueryIdRespStandardScheme();
        }
    }

    private static class TGetQueryIdRespStandardScheme
            extends org.apache.thrift.scheme.StandardScheme<TGetQueryIdResp> {

        public void read(org.apache.thrift.protocol.TProtocol iprot, TGetQueryIdResp struct)
                throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                    case 1: // QUERY_ID
                        if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                            struct.queryId = iprot.readString();
                            struct.setQueryIdIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    default:
                        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                }
                iprot.readFieldEnd();
            }
            iprot.readStructEnd();
            struct.validate();
        }

        public void write(org.apache.thrift.protocol.TProtocol oprot, TGetQueryIdResp struct)
                throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            if (struct.queryId != null) {
                oprot.writeFieldBegin(QUERY_ID_FIELD_DESC);
                oprot.writeString(struct.queryId);
                oprot.writeFieldEnd();
            }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }
    }

    private static class TGetQueryIdRespTupleSchemeFactory
            implements org.apache.thrift.scheme.SchemeFactory {
        public TGetQueryIdRespTupleScheme getScheme() {
            return new TGetQueryIdRespTupleScheme();
        }
    }

    private static class TGetQueryIdRespTupleScheme
            extends org.apache.thrift.scheme.TupleScheme<TGetQueryIdResp> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot, TGetQueryIdResp struct)
                throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TTupleProtocol oprot =
                    (org.apache.thrift.protocol.TTupleProtocol) prot;
            oprot.writeString(struct.queryId);
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot, TGetQueryIdResp struct)
                throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TTupleProtocol iprot =
                    (org.apache.thrift.protocol.TTupleProtocol) prot;
            struct.queryId = iprot.readString();
            struct.setQueryIdIsSet(true);
        }
    }

    private static <S extends org.apache.thrift.scheme.IScheme> S scheme(
            org.apache.thrift.protocol.TProtocol proto) {
        return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme())
                        ? STANDARD_SCHEME_FACTORY
                        : TUPLE_SCHEME_FACTORY)
                .getScheme();
    }
}
