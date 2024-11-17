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
public class TSetClientInfoReq
        implements org.apache.thrift.TBase<TSetClientInfoReq, TSetClientInfoReq._Fields>,
                java.io.Serializable,
                Cloneable,
                Comparable<TSetClientInfoReq> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC =
            new org.apache.thrift.protocol.TStruct("TSetClientInfoReq");

    private static final org.apache.thrift.protocol.TField SESSION_HANDLE_FIELD_DESC =
            new org.apache.thrift.protocol.TField(
                    "sessionHandle", org.apache.thrift.protocol.TType.STRUCT, (short) 1);
    private static final org.apache.thrift.protocol.TField CONFIGURATION_FIELD_DESC =
            new org.apache.thrift.protocol.TField(
                    "configuration", org.apache.thrift.protocol.TType.MAP, (short) 2);

    private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY =
            new TSetClientInfoReqStandardSchemeFactory();
    private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY =
            new TSetClientInfoReqTupleSchemeFactory();

    private TSessionHandle sessionHandle; // required
    private java.util.Map<java.lang.String, java.lang.String> configuration; // optional

    /**
     * The set of fields this struct contains, along with convenience methods for finding and
     * manipulating them.
     */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        SESSION_HANDLE((short) 1, "sessionHandle"),
        CONFIGURATION((short) 2, "configuration");

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
                case 1: // SESSION_HANDLE
                    return SESSION_HANDLE;
                case 2: // CONFIGURATION
                    return CONFIGURATION;
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
    private static final _Fields optionals[] = {_Fields.CONFIGURATION};
    public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData>
            metaDataMap;

    static {
        java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap =
                new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
                        _Fields.class);
        tmpMap.put(
                _Fields.SESSION_HANDLE,
                new org.apache.thrift.meta_data.FieldMetaData(
                        "sessionHandle",
                        org.apache.thrift.TFieldRequirementType.REQUIRED,
                        new org.apache.thrift.meta_data.StructMetaData(
                                org.apache.thrift.protocol.TType.STRUCT, TSessionHandle.class)));
        tmpMap.put(
                _Fields.CONFIGURATION,
                new org.apache.thrift.meta_data.FieldMetaData(
                        "configuration",
                        org.apache.thrift.TFieldRequirementType.OPTIONAL,
                        new org.apache.thrift.meta_data.MapMetaData(
                                org.apache.thrift.protocol.TType.MAP,
                                new org.apache.thrift.meta_data.FieldValueMetaData(
                                        org.apache.thrift.protocol.TType.STRING),
                                new org.apache.thrift.meta_data.FieldValueMetaData(
                                        org.apache.thrift.protocol.TType.STRING))));
        metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
                TSetClientInfoReq.class, metaDataMap);
    }

    public TSetClientInfoReq() {}

    public TSetClientInfoReq(TSessionHandle sessionHandle) {
        this();
        this.sessionHandle = sessionHandle;
    }

    /** Performs a deep copy on <i>other</i>. */
    public TSetClientInfoReq(TSetClientInfoReq other) {
        if (other.isSetSessionHandle()) {
            this.sessionHandle = new TSessionHandle(other.sessionHandle);
        }
        if (other.isSetConfiguration()) {
            java.util.Map<java.lang.String, java.lang.String> __this__configuration =
                    new java.util.HashMap<java.lang.String, java.lang.String>(other.configuration);
            this.configuration = __this__configuration;
        }
    }

    public TSetClientInfoReq deepCopy() {
        return new TSetClientInfoReq(this);
    }

    @Override
    public void clear() {
        this.sessionHandle = null;
        this.configuration = null;
    }

    public TSessionHandle getSessionHandle() {
        return this.sessionHandle;
    }

    public void setSessionHandle(TSessionHandle sessionHandle) {
        this.sessionHandle = sessionHandle;
    }

    public void unsetSessionHandle() {
        this.sessionHandle = null;
    }

    /**
     * Returns true if field sessionHandle is set (has been assigned a value) and false otherwise
     */
    public boolean isSetSessionHandle() {
        return this.sessionHandle != null;
    }

    public void setSessionHandleIsSet(boolean value) {
        if (!value) {
            this.sessionHandle = null;
        }
    }

    public int getConfigurationSize() {
        return (this.configuration == null) ? 0 : this.configuration.size();
    }

    public void putToConfiguration(java.lang.String key, java.lang.String val) {
        if (this.configuration == null) {
            this.configuration = new java.util.HashMap<java.lang.String, java.lang.String>();
        }
        this.configuration.put(key, val);
    }

    public java.util.Map<java.lang.String, java.lang.String> getConfiguration() {
        return this.configuration;
    }

    public void setConfiguration(java.util.Map<java.lang.String, java.lang.String> configuration) {
        this.configuration = configuration;
    }

    public void unsetConfiguration() {
        this.configuration = null;
    }

    /**
     * Returns true if field configuration is set (has been assigned a value) and false otherwise
     */
    public boolean isSetConfiguration() {
        return this.configuration != null;
    }

    public void setConfigurationIsSet(boolean value) {
        if (!value) {
            this.configuration = null;
        }
    }

    public void setFieldValue(_Fields field, java.lang.Object value) {
        switch (field) {
            case SESSION_HANDLE:
                if (value == null) {
                    unsetSessionHandle();
                } else {
                    setSessionHandle((TSessionHandle) value);
                }
                break;

            case CONFIGURATION:
                if (value == null) {
                    unsetConfiguration();
                } else {
                    setConfiguration((java.util.Map<java.lang.String, java.lang.String>) value);
                }
                break;
        }
    }

    public java.lang.Object getFieldValue(_Fields field) {
        switch (field) {
            case SESSION_HANDLE:
                return getSessionHandle();

            case CONFIGURATION:
                return getConfiguration();
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
            case SESSION_HANDLE:
                return isSetSessionHandle();
            case CONFIGURATION:
                return isSetConfiguration();
        }
        throw new java.lang.IllegalStateException();
    }

    @Override
    public boolean equals(java.lang.Object that) {
        if (that == null) return false;
        if (that instanceof TSetClientInfoReq) return this.equals((TSetClientInfoReq) that);
        return false;
    }

    public boolean equals(TSetClientInfoReq that) {
        if (that == null) return false;
        if (this == that) return true;

        boolean this_present_sessionHandle = true && this.isSetSessionHandle();
        boolean that_present_sessionHandle = true && that.isSetSessionHandle();
        if (this_present_sessionHandle || that_present_sessionHandle) {
            if (!(this_present_sessionHandle && that_present_sessionHandle)) return false;
            if (!this.sessionHandle.equals(that.sessionHandle)) return false;
        }

        boolean this_present_configuration = true && this.isSetConfiguration();
        boolean that_present_configuration = true && that.isSetConfiguration();
        if (this_present_configuration || that_present_configuration) {
            if (!(this_present_configuration && that_present_configuration)) return false;
            if (!this.configuration.equals(that.configuration)) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 1;

        hashCode = hashCode * 8191 + ((isSetSessionHandle()) ? 131071 : 524287);
        if (isSetSessionHandle()) hashCode = hashCode * 8191 + sessionHandle.hashCode();

        hashCode = hashCode * 8191 + ((isSetConfiguration()) ? 131071 : 524287);
        if (isSetConfiguration()) hashCode = hashCode * 8191 + configuration.hashCode();

        return hashCode;
    }

    @Override
    public int compareTo(TSetClientInfoReq other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison =
                java.lang.Boolean.valueOf(isSetSessionHandle())
                        .compareTo(other.isSetSessionHandle());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetSessionHandle()) {
            lastComparison =
                    org.apache.thrift.TBaseHelper.compareTo(
                            this.sessionHandle, other.sessionHandle);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison =
                java.lang.Boolean.valueOf(isSetConfiguration())
                        .compareTo(other.isSetConfiguration());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetConfiguration()) {
            lastComparison =
                    org.apache.thrift.TBaseHelper.compareTo(
                            this.configuration, other.configuration);
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
        java.lang.StringBuilder sb = new java.lang.StringBuilder("TSetClientInfoReq(");
        boolean first = true;

        sb.append("sessionHandle:");
        if (this.sessionHandle == null) {
            sb.append("null");
        } else {
            sb.append(this.sessionHandle);
        }
        first = false;
        if (isSetConfiguration()) {
            if (!first) sb.append(", ");
            sb.append("configuration:");
            if (this.configuration == null) {
                sb.append("null");
            } else {
                sb.append(this.configuration);
            }
            first = false;
        }
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        if (!isSetSessionHandle()) {
            throw new org.apache.thrift.protocol.TProtocolException(
                    "Required field 'sessionHandle' is unset! Struct:" + toString());
        }

        // check for sub-struct validity
        if (sessionHandle != null) {
            sessionHandle.validate();
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

    private static class TSetClientInfoReqStandardSchemeFactory
            implements org.apache.thrift.scheme.SchemeFactory {
        public TSetClientInfoReqStandardScheme getScheme() {
            return new TSetClientInfoReqStandardScheme();
        }
    }

    private static class TSetClientInfoReqStandardScheme
            extends org.apache.thrift.scheme.StandardScheme<TSetClientInfoReq> {

        public void read(org.apache.thrift.protocol.TProtocol iprot, TSetClientInfoReq struct)
                throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                    case 1: // SESSION_HANDLE
                        if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
                            struct.sessionHandle = new TSessionHandle();
                            struct.sessionHandle.read(iprot);
                            struct.setSessionHandleIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 2: // CONFIGURATION
                        if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
                            {
                                org.apache.thrift.protocol.TMap _map162 = iprot.readMapBegin();
                                struct.configuration =
                                        new java.util.HashMap<java.lang.String, java.lang.String>(
                                                2 * _map162.size);
                                java.lang.String _key163;
                                java.lang.String _val164;
                                for (int _i165 = 0; _i165 < _map162.size; ++_i165) {
                                    _key163 = iprot.readString();
                                    _val164 = iprot.readString();
                                    struct.configuration.put(_key163, _val164);
                                }
                                iprot.readMapEnd();
                            }
                            struct.setConfigurationIsSet(true);
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

        public void write(org.apache.thrift.protocol.TProtocol oprot, TSetClientInfoReq struct)
                throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            if (struct.sessionHandle != null) {
                oprot.writeFieldBegin(SESSION_HANDLE_FIELD_DESC);
                struct.sessionHandle.write(oprot);
                oprot.writeFieldEnd();
            }
            if (struct.configuration != null) {
                if (struct.isSetConfiguration()) {
                    oprot.writeFieldBegin(CONFIGURATION_FIELD_DESC);
                    {
                        oprot.writeMapBegin(
                                new org.apache.thrift.protocol.TMap(
                                        org.apache.thrift.protocol.TType.STRING,
                                        org.apache.thrift.protocol.TType.STRING,
                                        struct.configuration.size()));
                        for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter166 :
                                struct.configuration.entrySet()) {
                            oprot.writeString(_iter166.getKey());
                            oprot.writeString(_iter166.getValue());
                        }
                        oprot.writeMapEnd();
                    }
                    oprot.writeFieldEnd();
                }
            }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }
    }

    private static class TSetClientInfoReqTupleSchemeFactory
            implements org.apache.thrift.scheme.SchemeFactory {
        public TSetClientInfoReqTupleScheme getScheme() {
            return new TSetClientInfoReqTupleScheme();
        }
    }

    private static class TSetClientInfoReqTupleScheme
            extends org.apache.thrift.scheme.TupleScheme<TSetClientInfoReq> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot, TSetClientInfoReq struct)
                throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TTupleProtocol oprot =
                    (org.apache.thrift.protocol.TTupleProtocol) prot;
            struct.sessionHandle.write(oprot);
            java.util.BitSet optionals = new java.util.BitSet();
            if (struct.isSetConfiguration()) {
                optionals.set(0);
            }
            oprot.writeBitSet(optionals, 1);
            if (struct.isSetConfiguration()) {
                {
                    oprot.writeI32(struct.configuration.size());
                    for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter167 :
                            struct.configuration.entrySet()) {
                        oprot.writeString(_iter167.getKey());
                        oprot.writeString(_iter167.getValue());
                    }
                }
            }
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot, TSetClientInfoReq struct)
                throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TTupleProtocol iprot =
                    (org.apache.thrift.protocol.TTupleProtocol) prot;
            struct.sessionHandle = new TSessionHandle();
            struct.sessionHandle.read(iprot);
            struct.setSessionHandleIsSet(true);
            java.util.BitSet incoming = iprot.readBitSet(1);
            if (incoming.get(0)) {
                {
                    org.apache.thrift.protocol.TMap _map168 =
                            new org.apache.thrift.protocol.TMap(
                                    org.apache.thrift.protocol.TType.STRING,
                                    org.apache.thrift.protocol.TType.STRING,
                                    iprot.readI32());
                    struct.configuration =
                            new java.util.HashMap<java.lang.String, java.lang.String>(
                                    2 * _map168.size);
                    java.lang.String _key169;
                    java.lang.String _val170;
                    for (int _i171 = 0; _i171 < _map168.size; ++_i171) {
                        _key169 = iprot.readString();
                        _val170 = iprot.readString();
                        struct.configuration.put(_key169, _val170);
                    }
                }
                struct.setConfigurationIsSet(true);
            }
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
