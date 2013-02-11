package conduit.amqp;

import conduit.transport.TransportConnectionProperties;

/**
 * User: kmandrika
 * Date: 1/8/13
 */
public class AMQPConnectionProperties implements TransportConnectionProperties {
    private String username;
    private String password;
    private String virtualHost;
    private int connectionTimeout;
    private int heartbeatInterval;

    public AMQPConnectionProperties(String username, String password, String virtualHost) {
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.connectionTimeout = 0; //! 0 is infinite.
        this.heartbeatInterval = 60; //! In seconds.
    }

    public AMQPConnectionProperties(String username, String password) {
        this(username, password, "/");
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
}
