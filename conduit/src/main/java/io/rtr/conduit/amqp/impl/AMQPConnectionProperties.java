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

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder virtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder connectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder heartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder automaticRecoveryEnabled(boolean automaticRecoveryEnabled) {
            this.automaticRecoveryEnabled = automaticRecoveryEnabled;
            return this;
        }

        public Builder networkRecoveryInterval(long networkRecoveryInterval) {
            this.networkRecoveryInterval = networkRecoveryInterval;
            return this;
        }

        public Builder topologyRecoveryInterval(Long topologyRecoveryInterval) {
            this.topologyRecoveryInterval = topologyRecoveryInterval;
            return this;
        }

        public Builder topologyRecoveryMaxAttempts(Integer topologyRecoveryMaxAttempts) {
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

    AMQPConnectionProperties(String username, String password) {
        this(username, password, "/");
    }

    AMQPConnectionProperties(String username, String password, String virtualHost) {
        this(username, password, virtualHost, 10000, 60, true);
    }

    AMQPConnectionProperties(
            String username,
            String password,
            String virtualHost,
            int connectionTimeout,
            int heartbeatInterval) {
        // Different default automaticRecoveryEnabled for this constructor is weird, but it was
        // preexisting logic retained for compatibility
        this(username, password, virtualHost, connectionTimeout, heartbeatInterval, false);
    }

    AMQPConnectionProperties(
            String username,
            String password,
            String virtualHost,
            int connectionTimeout,
            int heartbeatInterval,
            boolean automaticRecoveryEnabled) {
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.connectionTimeout = connectionTimeout;
        this.heartbeatInterval = heartbeatInterval;
        this.automaticRecoveryEnabled = automaticRecoveryEnabled;
    }

    AMQPConnectionProperties(
            String username,
            String password,
            String virtualHost,
            int connectionTimeout,
            int heartbeatInterval,
            boolean automaticRecoveryEnabled,
            long networkRecoveryInterval,
            Long topologyRecoveryInterval,
            Integer topologyRecoveryMaxAttempts) {
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
