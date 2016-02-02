package com.barley.orleans.structure;

/**
 * Builder implementation for Payload class.
 */
public class PayloadBuilder {
    private String client;
    private String ipAddress;
    private String uuid;
    private String schemaId;
    private Object data;

    private PayloadBuilder() {
    }

    public static PayloadBuilder aPayload() {
        return new PayloadBuilder();
    }

    public PayloadBuilder withClient(String client) {
        this.client = client;
        return this;
    }

    public PayloadBuilder withIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
        return this;
    }

    public PayloadBuilder withUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public PayloadBuilder withSchemaId(String schemaId) {
        this.schemaId = schemaId;
        return this;
    }

    public PayloadBuilder withData(Object data) {
        this.data = data;
        return this;
    }

    public PayloadBuilder but() {
        return aPayload().withClient(client).withIpAddress(ipAddress).withUuid(uuid).withSchemaId(schemaId).withData(data);
    }

    public Payload build() {
        Payload payload = new Payload();
        payload.setClient(client);
        payload.setIpAddress(ipAddress);
        payload.setUuid(uuid);
        payload.setSchemaId(schemaId);
        payload.setData(data);
        return payload;
    }
}
