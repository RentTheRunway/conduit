package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.transport.TransportConnectionProperties;

import java.time.Duration;

public class AMQPConnectionProperties implements TransportConnectionProperties {

    public static class Builder {
        private String username;
        private String password;
        private String virtualHost = "/";
        private Duration connectionTimeout = Duration.ofSeconds(10);
        private Duration heartbeatInterval = Duration.ofSeconds(60);
        private boolean automaticRecoveryEnabled;
        private long networkRecoveryInterval = 5000L;
        private Long topologyRecoveryInterval;
        private Integer topologyRecoveryMaxAttempts;

        private Builder() {}

        public Builder username(final String username) {
            this.username = username;
            return this;
        }

        public Builder password(final String password) {
            this.password = password;
            return this;
        }

        public Builder virtualHost(final String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder connectionTimeout(final Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder heartbeatInterval(final Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder automaticRecoveryEnabled(final boolean automaticRecoveryEnabled) {
            this.automaticRecoveryEnabled = automaticRecoveryEnabled;
            return this;
        }

        public Builder networkRecoveryInterval(final long networkRecoveryInterval) {
            this.networkRecoveryInterval = networkRecoveryInterval;
            return this;
        }

        public Builder topologyRecoveryInterval(final Long topologyRecoveryInterval) {
            this.topologyRecoveryInterval = topologyRecoveryInterval;
            return this;
        }

        public Builder topologyRecoveryMaxAttempts(final Integer topologyRecoveryMaxAttempts) {
            this.topologyRecoveryMaxAttempts = topologyRecoveryMaxAttempts;
            return builder();
        }

        public AMQPConnectionProperties build() {
            return new AMQPConnectionProperties(
                    username,
                    password,
                    virtualHost,
                    (int) connectionTimeout.toMillis(),
                    (int) heartbeatInterval.getSeconds(),
                    automaticRecoveryEnabled,
                    networkRecoveryInterval,
                    topologyRecoveryInterval,
                    topologyRecoveryMaxAttempts);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final String username;
    private final String password;
    private final String virtualHost;
    private final int connectionTimeout;
    private final int heartbeatInterval;
    private final boolean automaticRecoveryEnabled;
    private long networkRecoveryInterval;
    private Long topologyRecoveryInterval;
    private Integer topologyRecoveryMaxAttempts = Integer.MAX_VALUE;

    AMQPConnectionProperties(final String username, final String password) {
        this(username, password, "/");
    }

    AMQPConnectionProperties(
            final String username, final String password, final String virtualHost) {
        this(username, password, virtualHost, 10000, 60, true);
    }

    AMQPConnectionProperties(
            final String username,
            final String password,
            final String virtualHost,
            final int connectionTimeout,
            final int heartbeatInterval) {
        // Different default automaticRecoveryEnabled for this constructor is weird, but it was
        // preexisting logic retained for compatibility
        this(username, password, virtualHost, connectionTimeout, heartbeatInterval, false);
    }

    AMQPConnectionProperties(
            final String username,
            final String password,
            final String virtualHost,
            final int connectionTimeout,
            final int heartbeatInterval,
            final boolean automaticRecoveryEnabled) {
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.connectionTimeout = connectionTimeout;
        this.heartbeatInterval = heartbeatInterval;
        this.automaticRecoveryEnabled = automaticRecoveryEnabled;
    }

    AMQPConnectionProperties(
            final String username,
            final String password,
            final String virtualHost,
            final int connectionTimeout,
            final int heartbeatInterval,
            final boolean automaticRecoveryEnabled,
            final long networkRecoveryInterval,
            final Long topologyRecoveryInterval,
            final Integer topologyRecoveryMaxAttempts) {
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.connectionTimeout = connectionTimeout;
        this.heartbeatInterval = heartbeatInterval;
        this.automaticRecoveryEnabled = automaticRecoveryEnabled;
        this.networkRecoveryInterval = networkRecoveryInterval;
        this.topologyRecoveryInterval = topologyRecoveryInterval;
        this.topologyRecoveryMaxAttempts = topologyRecoveryMaxAttempts;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public boolean isAutomaticRecoveryEnabled() {
        return automaticRecoveryEnabled;
    }

    public long getNetworkRecoveryInterval() {
        return networkRecoveryInterval;
    }

    public Long getTopologyRecoveryInterval() {
        return topologyRecoveryInterval;
    }

    public Integer getTopologyRecoveryMaxAttempts() {
        return topologyRecoveryMaxAttempts;
    }
}
