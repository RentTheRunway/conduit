package io.rtr.conduit.amqp.connection;

import com.rabbitmq.client.ConnectionFactory;

import javax.net.SocketFactory;
import java.util.concurrent.ThreadFactory;

public class RabbitAMQPConnectionBuilder{

    private ConnectionFactory factory = new ConnectionFactory();

    private SocketFactory setSocketFactory;
    private String host;
    private int port;
    private ThreadFactory daemonThreadFactory;
    private String username;
    private String password;
    private String virtualHost;
    private int connectionTimeout;
    private int heartbeatInterval;
    private boolean automaticRecoveryEnabled;

    public RabbitAMQPConnectionBuilder() {
    }

    public static RabbitAMQPConnectionBuilder builder() {
        return new RabbitAMQPConnectionBuilder();
    }


    public void setSocketFactory(SocketFactory socketFactory) {
        this.setSocketFactory = socketFactory;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setThreadFactory(ThreadFactory daemonThreadFactory) {
        this.daemonThreadFactory = daemonThreadFactory;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public void setRequestedHeartbeat(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public void setAutomaticRecoveryEnabled(boolean automaticRecoveryEnabled) {
        this.automaticRecoveryEnabled = automaticRecoveryEnabled;
    }

    public RabbitAMQPConnection build() {
        this.factory.setSocketFactory(this.setSocketFactory);
        this.factory.setHost(this.host);
        this.factory.setPort(this.port);
        this.factory.setThreadFactory(this.daemonThreadFactory);
        this.factory.setUsername(this.username);
        this.factory.setPassword(this.password);
        this.factory.setVirtualHost(this.virtualHost);
        this.factory.setConnectionTimeout(this.connectionTimeout);
        this.factory.setRequestedHeartbeat(this.heartbeatInterval);
        this.factory.setAutomaticRecoveryEnabled(this.automaticRecoveryEnabled);
        return new RabbitAMQPConnection(this.factory);
    }
}
