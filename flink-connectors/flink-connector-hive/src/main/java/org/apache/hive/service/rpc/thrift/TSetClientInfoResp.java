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
public class TSetClientInfoResp
        implements org.apache.thrift.TBase<TSetClientInfoResp, TSetClientInfoResp._Fields>,
                java.io.Serializable,
                Cloneable,
                Comparable<TSetClientInfoResp> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC =
            new org.apache.thrift.protocol.TStruct("TSetClientInfoResp");

    private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC =
            new org.apache.thrift.protocol.TField(
                    "status", org.apache.thrift.protocol.TType.STRUCT, (short) 1);

    private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY =
            new TSetClientInfoRespStandardSchemeFactory();
    private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY =
            new TSetClientInfoRespTupleSchemeFactory();

    private TStatus status; // required

    /**
     * The set of fields this struct contains, along with convenience methods for finding and
     * manipulating them.
     */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        STATUS((short) 1, "status");

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
                case 1: // STATUS
                    return STATUS;
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
                _Fields.STATUS,
                new org.apache.thrift.meta_data.FieldMetaData(
                        "status",
                        org.apache.thrift.TFieldRequirementType.REQUIRED,
                        new org.apache.thrift.meta_data.StructMetaData(
                                org.apache.thrift.protocol.TType.STRUCT, TStatus.class)));
        metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
                TSetClientInfoResp.class, metaDataMap);
    }

    public TSetClientInfoResp() {}

    public TSetClientInfoResp(TStatus status) {
        this();
        this.status = status;
    }

    /** Performs a deep copy on <i>other</i>. */
    public TSetClientInfoResp(TSetClientInfoResp other) {
        if (other.isSetStatus()) {
            this.status = new TStatus(other.status);
        }
    }

    public TSetClientInfoResp deepCopy() {
        return new TSetClientInfoResp(this);
    }

    @Override
    public void clear() {
        this.status = null;
    }

    public TStatus getStatus() {
        return this.status;
    }

    public void setStatus(TStatus status) {
        this.status = status;
    }

    public void unsetStatus() {
        this.status = null;
    }

    /** Returns true if field status is set (has been assigned a value) and false otherwise */
    public boolean isSetStatus() {
        return this.status != null;
    }

    public void setStatusIsSet(boolean value) {
        if (!value) {
            this.status = null;
        }
    }

    public void setFieldValue(_Fields field, java.lang.Object value) {
        switch (field) {
            case STATUS:
                if (value == null) {
                    unsetStatus();
                } else {
                    setStatus((TStatus) value);
                }
                break;
        }
    }

    public java.lang.Object getFieldValue(_Fields field) {
        switch (field) {
            case STATUS:
                return getStatus();
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
            case STATUS:
                return isSetStatus();
        }
        throw new java.lang.IllegalStateException();
    }

    @Override
    public boolean equals(java.lang.Object that) {
        if (that == null) return false;
        if (that instanceof TSetClientInfoResp) return this.equals((TSetClientInfoResp) that);
        return false;
    }

    public boolean equals(TSetClientInfoResp that) {
        if (that == null) return false;
        if (this == that) return true;

        boolean this_present_status = true && this.isSetStatus();
        boolean that_present_status = true && that.isSetStatus();
        if (this_present_status || that_present_status) {
            if (!(this_present_status && that_present_status)) return false;
            if (!this.status.equals(that.status)) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 1;

        hashCode = hashCode * 8191 + ((isSetStatus()) ? 131071 : 524287);
        if (isSetStatus()) hashCode = hashCode * 8191 + status.hashCode();

        return hashCode;
    }

    @Override
    public int compareTo(TSetClientInfoResp other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = java.lang.Boolean.valueOf(isSetStatus()).compareTo(other.isSetStatus());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetStatus()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status, other.status);
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
        java.lang.StringBuilder sb = new java.lang.StringBuilder("TSetClientInfoResp(");
        boolean first = true;

        sb.append("status:");
        if (this.status == null) {
            sb.append("null");
        } else {
            sb.append(this.status);
        }
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        if (!isSetStatus()) {
            throw new org.apache.thrift.protocol.TProtocolException(
                    "Required field 'status' is unset! Struct:" + toString());
        }

        // check for sub-struct validity
        if (status != null) {
            status.validate();
        }
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

    private static class TSetClientInfoRespStandardSchemeFactory
            implements org.apache.thrift.scheme.SchemeFactory {
        public TSetClientInfoRespStandardScheme getScheme() {
            return new TSetClientInfoRespStandardScheme();
        }
    }

    private static class TSetClientInfoRespStandardScheme
            extends org.apache.thrift.scheme.StandardScheme<TSetClientInfoResp> {

        public void read(org.apache.thrift.protocol.TProtocol iprot, TSetClientInfoResp struct)
                throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                    case 1: // STATUS
                        if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
                            struct.status = new TStatus();
                            struct.status.read(iprot);
                            struct.setStatusIsSet(true);
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

        public void write(org.apache.thrift.protocol.TProtocol oprot, TSetClientInfoResp struct)
                throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            if (struct.status != null) {
                oprot.writeFieldBegin(STATUS_FIELD_DESC);
                struct.status.write(oprot);
                oprot.writeFieldEnd();
            }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }
    }

    private static class TSetClientInfoRespTupleSchemeFactory
            implements org.apache.thrift.scheme.SchemeFactory {
        public TSetClientInfoRespTupleScheme getScheme() {
            return new TSetClientInfoRespTupleScheme();
        }
    }

    private static class TSetClientInfoRespTupleScheme
            extends org.apache.thrift.scheme.TupleScheme<TSetClientInfoResp> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot, TSetClientInfoResp struct)
                throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TTupleProtocol oprot =
                    (org.apache.thrift.protocol.TTupleProtocol) prot;
            struct.status.write(oprot);
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot, TSetClientInfoResp struct)
                throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TTupleProtocol iprot =
                    (org.apache.thrift.protocol.TTupleProtocol) prot;
            struct.status = new TStatus();
            struct.status.read(iprot);
            struct.setStatusIsSet(true);
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
