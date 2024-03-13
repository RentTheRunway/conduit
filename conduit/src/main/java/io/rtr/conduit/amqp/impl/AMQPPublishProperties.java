package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.transport.TransportPublishProperties;

public class AMQPPublishProperties implements TransportPublishProperties {
    private final String exchange;
    private final String routingKey;
    private final long timeout;
    private final boolean confirmEnabled;

    private AMQPPublishProperties(final Builder builder) {
        this.exchange = builder.exchange;
        this.routingKey = builder.routingKey;
        this.timeout = builder.timeout;
        this.confirmEnabled = builder.confirmEnabled;
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isConfirmEnabled() {
        return confirmEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String exchange;
        private String routingKey;
        private long timeout;
        private boolean confirmEnabled;

        public Builder exchange(final String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder routingKey(final String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        public Builder timeout(final long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder confirmEnabled(final boolean confirmEnabled) {
            this.confirmEnabled = confirmEnabled;
            return this;
        }

        public Builder of(final AMQPPublishProperties base) {
            return exchange(base.getExchange())
                    .routingKey(base.getRoutingKey())
                    .timeout(base.getTimeout())
                    .confirmEnabled(base.isConfirmEnabled());
        }

        public AMQPPublishProperties build() {
            return new AMQPPublishProperties(this);
        }
    }
}
