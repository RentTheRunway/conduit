package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.transport.TransportConnectionProperties;

public class AMQPConnectionProperties implements TransportConnectionProperties {
    private String username;
    private String password;
    private String virtualHost;
    private int connectionTimeout;
    private int heartbeatInterval;
    private boolean automaticRecoveryEnabled;

    AMQPConnectionProperties(String username, String password, String virtualHost) {
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.connectionTimeout = 10000; //! In milliseconds.
        this.heartbeatInterval = 60; //! In seconds.
        this.automaticRecoveryEnabled = true;
    }

    AMQPConnectionProperties(String username, String password) {
        this(username, password, "/");
    }

    AMQPConnectionProperties(String username
                                  , String password
                                  , String virtualHost
                                  , int connectionTimeout
                                  , int heartbeatInterval) {
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.connectionTimeout = connectionTimeout;
        this.heartbeatInterval = heartbeatInterval;
    }

    AMQPConnectionProperties(String username
                                  , String password
                                  , String virtualHost
                                  , int connectionTimeout
                                  , int heartbeatInterval
                                  , boolean automaticRecoveryEnabled) {
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.connectionTimeout = connectionTimeout;
        this.heartbeatInterval = heartbeatInterval;
        this.automaticRecoveryEnabled = automaticRecoveryEnabled;
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
}
