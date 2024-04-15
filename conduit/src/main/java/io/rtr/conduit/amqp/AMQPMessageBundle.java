package io.rtr.conduit.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import io.rtr.conduit.amqp.transport.TransportMessageBundle;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AMQPMessageBundle implements TransportMessageBundle {
    private final String consumerTag;
    private final Envelope envelope;
    private final AMQP.BasicProperties basicProperties;
    private final byte[] body;

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

    private AMQPMessageBundle(final Builder builder) {
        this.consumerTag = builder.consumerTag;
        this.envelope = builder.envelope;
        this.basicProperties = builder.basicProperties;
        this.body = builder.body;
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String consumerTag;
        private Envelope envelope;
        private AMQP.BasicProperties basicProperties;
        private Map<String, Object> headers = new HashMap<>();
        private byte[] body;

        public Builder consumerTag(final String consumerTag) {
            this.consumerTag = consumerTag;
            return this;
        }

        public Builder envelope(final Envelope envelope) {
            this.envelope = envelope;
            return this;
        }

        public Builder basicProperties(final AMQP.BasicProperties basicProperties) {
            this.basicProperties = basicProperties;
            return this;
        }

        public Builder headers(final Map<String, Object> headers) {
            if (headers != null) {
                this.headers = new HashMap<>(headers);
            }
            return this;
        }

        public Builder header(final String name, final Object value) {
            if (value == null) {
                this.headers.remove(name);
            } else {
                this.headers.put(name, value);
            }
            return this;
        }

        public Builder body(final byte[] body) {
            this.body = body;
            return this;
        }

        public Builder body(final String body) {
            return body(body.getBytes());
        }

        public AMQPMessageBundle build() {
            if (basicProperties == null) {
                this.basicProperties = initialProperties(headers);
            } else if (!headers.isEmpty()) {
                throw new IllegalArgumentException("Both basicProperties and headers are set");
            }
            return new AMQPMessageBundle(this);
        }
    }
}
