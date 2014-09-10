package io.rtr.conduit.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import io.rtr.conduit.amqp.transport.TransportMessageBundle;

import java.util.HashMap;
import java.util.Map;

public class AMQPMessageBundle implements TransportMessageBundle {
    private String consumerTag;
    private Envelope envelope;
    private AMQP.BasicProperties basicProperties;
    private byte[] body;

    private static AMQP.BasicProperties initialProperties() {
        return initialProperties(null);
    }

    private static AMQP.BasicProperties initialProperties(Map<String, Object> additionalHeaders) {
        Map<String, Object> headers = new HashMap<String, Object>();

        if (additionalHeaders != null) {
            headers.putAll(additionalHeaders);
        }

        headers.put("conduit-retry-count", 0);

        return new AMQP.BasicProperties()
                .builder()
                .deliveryMode(2 /*persistent*/)
                .priority(0)
                .headers(headers)
                .contentType("text/plain")
                .build();
    }

    public AMQPMessageBundle(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] body) {
        this.consumerTag = consumerTag;
        this.envelope = envelope;
        this.basicProperties = basicProperties;
        this.body = body;
    }

    public AMQPMessageBundle(String message) {
        this(null, null, initialProperties(), message.getBytes());
    }

    public AMQPMessageBundle(String message, Map<String, Object> headers) {
        this(null, null, initialProperties(headers), message.getBytes());
    }

    public String getConsumerTag() {
        return consumerTag;
    }

    public Envelope getEnvelope() {
        return envelope;
    }

    public AMQP.BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public byte[] getBody() {
        return body;
    }
}
