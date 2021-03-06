// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: response.proto

package server.messages;

public final class ResponseProto {
  private ResponseProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface ResponseOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required int64 id = 1;
    /**
     * <code>required int64 id = 1;</code>
     */
    boolean hasId();
    /**
     * <code>required int64 id = 1;</code>
     */
    long getId();

    // required string key = 2;
    /**
     * <code>required string key = 2;</code>
     */
    boolean hasKey();
    /**
     * <code>required string key = 2;</code>
     */
    java.lang.String getKey();
    /**
     * <code>required string key = 2;</code>
     */
    com.google.protobuf.ByteString
        getKeyBytes();

    // required int64 time = 3;
    /**
     * <code>required int64 time = 3;</code>
     */
    boolean hasTime();
    /**
     * <code>required int64 time = 3;</code>
     */
    long getTime();

    // required bool suc = 4;
    /**
     * <code>required bool suc = 4;</code>
     */
    boolean hasSuc();
    /**
     * <code>required bool suc = 4;</code>
     */
    boolean getSuc();

    // optional bytes value = 5;
    /**
     * <code>optional bytes value = 5;</code>
     */
    boolean hasValue();
    /**
     * <code>optional bytes value = 5;</code>
     */
    com.google.protobuf.ByteString getValue();

    // optional bytes old = 6;
    /**
     * <code>optional bytes old = 6;</code>
     */
    boolean hasOld();
    /**
     * <code>optional bytes old = 6;</code>
     */
    com.google.protobuf.ByteString getOld();
  }
  /**
   * Protobuf type {@code messages.Response}
   */
  public static final class Response extends
      com.google.protobuf.GeneratedMessage
      implements ResponseOrBuilder {
    // Use Response.newBuilder() to construct.
    private Response(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private Response(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final Response defaultInstance;
    public static Response getDefaultInstance() {
      return defaultInstance;
    }

    public Response getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private Response(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              id_ = input.readInt64();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              key_ = input.readBytes();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              time_ = input.readInt64();
              break;
            }
            case 32: {
              bitField0_ |= 0x00000008;
              suc_ = input.readBool();
              break;
            }
            case 42: {
              bitField0_ |= 0x00000010;
              value_ = input.readBytes();
              break;
            }
            case 50: {
              bitField0_ |= 0x00000020;
              old_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return server.messages.ResponseProto.internal_static_messages_Response_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return server.messages.ResponseProto.internal_static_messages_Response_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              server.messages.ResponseProto.Response.class, server.messages.ResponseProto.Response.Builder.class);
    }

    public static com.google.protobuf.Parser<Response> PARSER =
        new com.google.protobuf.AbstractParser<Response>() {
      public Response parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Response(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<Response> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required int64 id = 1;
    public static final int ID_FIELD_NUMBER = 1;
    private long id_;
    /**
     * <code>required int64 id = 1;</code>
     */
    public boolean hasId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required int64 id = 1;</code>
     */
    public long getId() {
      return id_;
    }

    // required string key = 2;
    public static final int KEY_FIELD_NUMBER = 2;
    private java.lang.Object key_;
    /**
     * <code>required string key = 2;</code>
     */
    public boolean hasKey() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required string key = 2;</code>
     */
    public java.lang.String getKey() {
      java.lang.Object ref = key_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          key_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string key = 2;</code>
     */
    public com.google.protobuf.ByteString
        getKeyBytes() {
      java.lang.Object ref = key_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        key_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // required int64 time = 3;
    public static final int TIME_FIELD_NUMBER = 3;
    private long time_;
    /**
     * <code>required int64 time = 3;</code>
     */
    public boolean hasTime() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>required int64 time = 3;</code>
     */
    public long getTime() {
      return time_;
    }

    // required bool suc = 4;
    public static final int SUC_FIELD_NUMBER = 4;
    private boolean suc_;
    /**
     * <code>required bool suc = 4;</code>
     */
    public boolean hasSuc() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>required bool suc = 4;</code>
     */
    public boolean getSuc() {
      return suc_;
    }

    // optional bytes value = 5;
    public static final int VALUE_FIELD_NUMBER = 5;
    private com.google.protobuf.ByteString value_;
    /**
     * <code>optional bytes value = 5;</code>
     */
    public boolean hasValue() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional bytes value = 5;</code>
     */
    public com.google.protobuf.ByteString getValue() {
      return value_;
    }

    // optional bytes old = 6;
    public static final int OLD_FIELD_NUMBER = 6;
    private com.google.protobuf.ByteString old_;
    /**
     * <code>optional bytes old = 6;</code>
     */
    public boolean hasOld() {
      return ((bitField0_ & 0x00000020) == 0x00000020);
    }
    /**
     * <code>optional bytes old = 6;</code>
     */
    public com.google.protobuf.ByteString getOld() {
      return old_;
    }

    private void initFields() {
      id_ = 0L;
      key_ = "";
      time_ = 0L;
      suc_ = false;
      value_ = com.google.protobuf.ByteString.EMPTY;
      old_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasKey()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasTime()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasSuc()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeInt64(1, id_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getKeyBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeInt64(3, time_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        output.writeBool(4, suc_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        output.writeBytes(5, value_);
      }
      if (((bitField0_ & 0x00000020) == 0x00000020)) {
        output.writeBytes(6, old_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(1, id_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getKeyBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(3, time_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(4, suc_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(5, value_);
      }
      if (((bitField0_ & 0x00000020) == 0x00000020)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(6, old_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static server.messages.ResponseProto.Response parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static server.messages.ResponseProto.Response parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static server.messages.ResponseProto.Response parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static server.messages.ResponseProto.Response parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static server.messages.ResponseProto.Response parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static server.messages.ResponseProto.Response parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static server.messages.ResponseProto.Response parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static server.messages.ResponseProto.Response parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static server.messages.ResponseProto.Response parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static server.messages.ResponseProto.Response parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(server.messages.ResponseProto.Response prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code messages.Response}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements server.messages.ResponseProto.ResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return server.messages.ResponseProto.internal_static_messages_Response_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return server.messages.ResponseProto.internal_static_messages_Response_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                server.messages.ResponseProto.Response.class, server.messages.ResponseProto.Response.Builder.class);
      }

      // Construct using server.messages.ResponseProto.Response.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        id_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        key_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        time_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000004);
        suc_ = false;
        bitField0_ = (bitField0_ & ~0x00000008);
        value_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000010);
        old_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000020);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return server.messages.ResponseProto.internal_static_messages_Response_descriptor;
      }

      public server.messages.ResponseProto.Response getDefaultInstanceForType() {
        return server.messages.ResponseProto.Response.getDefaultInstance();
      }

      public server.messages.ResponseProto.Response build() {
        server.messages.ResponseProto.Response result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public server.messages.ResponseProto.Response buildPartial() {
        server.messages.ResponseProto.Response result = new server.messages.ResponseProto.Response(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.id_ = id_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.key_ = key_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.time_ = time_;
        if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
          to_bitField0_ |= 0x00000008;
        }
        result.suc_ = suc_;
        if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
          to_bitField0_ |= 0x00000010;
        }
        result.value_ = value_;
        if (((from_bitField0_ & 0x00000020) == 0x00000020)) {
          to_bitField0_ |= 0x00000020;
        }
        result.old_ = old_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof server.messages.ResponseProto.Response) {
          return mergeFrom((server.messages.ResponseProto.Response)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(server.messages.ResponseProto.Response other) {
        if (other == server.messages.ResponseProto.Response.getDefaultInstance()) return this;
        if (other.hasId()) {
          setId(other.getId());
        }
        if (other.hasKey()) {
          bitField0_ |= 0x00000002;
          key_ = other.key_;
          onChanged();
        }
        if (other.hasTime()) {
          setTime(other.getTime());
        }
        if (other.hasSuc()) {
          setSuc(other.getSuc());
        }
        if (other.hasValue()) {
          setValue(other.getValue());
        }
        if (other.hasOld()) {
          setOld(other.getOld());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasId()) {
          
          return false;
        }
        if (!hasKey()) {
          
          return false;
        }
        if (!hasTime()) {
          
          return false;
        }
        if (!hasSuc()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        server.messages.ResponseProto.Response parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (server.messages.ResponseProto.Response) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required int64 id = 1;
      private long id_ ;
      /**
       * <code>required int64 id = 1;</code>
       */
      public boolean hasId() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required int64 id = 1;</code>
       */
      public long getId() {
        return id_;
      }
      /**
       * <code>required int64 id = 1;</code>
       */
      public Builder setId(long value) {
        bitField0_ |= 0x00000001;
        id_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int64 id = 1;</code>
       */
      public Builder clearId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        id_ = 0L;
        onChanged();
        return this;
      }

      // required string key = 2;
      private java.lang.Object key_ = "";
      /**
       * <code>required string key = 2;</code>
       */
      public boolean hasKey() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required string key = 2;</code>
       */
      public java.lang.String getKey() {
        java.lang.Object ref = key_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          key_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string key = 2;</code>
       */
      public com.google.protobuf.ByteString
          getKeyBytes() {
        java.lang.Object ref = key_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          key_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string key = 2;</code>
       */
      public Builder setKey(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        key_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string key = 2;</code>
       */
      public Builder clearKey() {
        bitField0_ = (bitField0_ & ~0x00000002);
        key_ = getDefaultInstance().getKey();
        onChanged();
        return this;
      }
      /**
       * <code>required string key = 2;</code>
       */
      public Builder setKeyBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        key_ = value;
        onChanged();
        return this;
      }

      // required int64 time = 3;
      private long time_ ;
      /**
       * <code>required int64 time = 3;</code>
       */
      public boolean hasTime() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>required int64 time = 3;</code>
       */
      public long getTime() {
        return time_;
      }
      /**
       * <code>required int64 time = 3;</code>
       */
      public Builder setTime(long value) {
        bitField0_ |= 0x00000004;
        time_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int64 time = 3;</code>
       */
      public Builder clearTime() {
        bitField0_ = (bitField0_ & ~0x00000004);
        time_ = 0L;
        onChanged();
        return this;
      }

      // required bool suc = 4;
      private boolean suc_ ;
      /**
       * <code>required bool suc = 4;</code>
       */
      public boolean hasSuc() {
        return ((bitField0_ & 0x00000008) == 0x00000008);
      }
      /**
       * <code>required bool suc = 4;</code>
       */
      public boolean getSuc() {
        return suc_;
      }
      /**
       * <code>required bool suc = 4;</code>
       */
      public Builder setSuc(boolean value) {
        bitField0_ |= 0x00000008;
        suc_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bool suc = 4;</code>
       */
      public Builder clearSuc() {
        bitField0_ = (bitField0_ & ~0x00000008);
        suc_ = false;
        onChanged();
        return this;
      }

      // optional bytes value = 5;
      private com.google.protobuf.ByteString value_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes value = 5;</code>
       */
      public boolean hasValue() {
        return ((bitField0_ & 0x00000010) == 0x00000010);
      }
      /**
       * <code>optional bytes value = 5;</code>
       */
      public com.google.protobuf.ByteString getValue() {
        return value_;
      }
      /**
       * <code>optional bytes value = 5;</code>
       */
      public Builder setValue(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000010;
        value_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes value = 5;</code>
       */
      public Builder clearValue() {
        bitField0_ = (bitField0_ & ~0x00000010);
        value_ = getDefaultInstance().getValue();
        onChanged();
        return this;
      }

      // optional bytes old = 6;
      private com.google.protobuf.ByteString old_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes old = 6;</code>
       */
      public boolean hasOld() {
        return ((bitField0_ & 0x00000020) == 0x00000020);
      }
      /**
       * <code>optional bytes old = 6;</code>
       */
      public com.google.protobuf.ByteString getOld() {
        return old_;
      }
      /**
       * <code>optional bytes old = 6;</code>
       */
      public Builder setOld(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000020;
        old_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes old = 6;</code>
       */
      public Builder clearOld() {
        bitField0_ = (bitField0_ & ~0x00000020);
        old_ = getDefaultInstance().getOld();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:messages.Response)
    }

    static {
      defaultInstance = new Response(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:messages.Response)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_messages_Response_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_messages_Response_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\016response.proto\022\010messages\"Z\n\010Response\022\n" +
      "\n\002id\030\001 \002(\003\022\013\n\003key\030\002 \002(\t\022\014\n\004time\030\003 \002(\003\022\013\n" +
      "\003suc\030\004 \002(\010\022\r\n\005value\030\005 \001(\014\022\013\n\003old\030\006 \001(\014B " +
      "\n\017server.messagesB\rResponseProto"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_messages_Response_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_messages_Response_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_messages_Response_descriptor,
              new java.lang.String[] { "Id", "Key", "Time", "Suc", "Value", "Old", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
