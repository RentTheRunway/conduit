package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MetricsCollector;
import io.rtr.conduit.amqp.transport.TransportExecutor;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class AMQPConnection {
    private final ConnectionFactory connectionFactory;
    private Connection connection;
    private TransportExecutor executor;
    private final Supplier<TransportExecutor> executorFactory;

    public AMQPConnection(boolean ssl, String host, int port, MetricsCollector metricsCollector) {
        this(new ConnectionFactory(), TransportExecutor::new, ssl, host, port, metricsCollector);
    }

    public AMQPConnection(ConnectionFactory factory, Supplier<TransportExecutor> executorFactory, boolean ssl, String host, int port, MetricsCollector metricsCollector) {
        this.connectionFactory = factory;
        this.executorFactory = executorFactory;
        if (ssl) {
            factory.setSocketFactory(SSLSocketFactory.getDefault());
        }

        factory.setHost(host);
        factory.setPort(port);

        if (metricsCollector != null) {
            factory.setMetricsCollector(metricsCollector);
        }
    }

    public boolean isConnected() {
        return this.connection != null && this.connection.isOpen();
    }

    public synchronized void connect(AMQPConnectionProperties properties) throws IOException, TimeoutException {
        if (isConnected()) {
            return;
        }
        configureConnectionFactory(properties);
        initializeExecutor();
        connection = connectionFactory.newConnection(executor);
    }

    public synchronized void disconnect() throws IOException {
        //! We are going to assume that closing an already closed
        //  connection is considered success.
        if (isConnected()) {
            try {
                connection.close(connectionFactory.getConnectionTimeout());
                connection = null;
            } catch (AlreadyClosedException ignored) {}
        }
        stopListening();
    }

    public Channel createChannel() throws IOException {
        if (!isConnected()) {
            throw new IllegalStateException("Attempted to create channel whilst disconnected.");
        }
        Channel channel = connection.createChannel();
        channel.basicQos(1);
        return channel;
    }

    public void stopListening() {
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
    }

    public boolean waitToStopListening(Duration waitFor) throws InterruptedException {
        if (executor != null) {
            return executor.awaitTermination(waitFor.toMillis(), TimeUnit.MILLISECONDS);
        }
        return true;
    }

    private void initializeExecutor() {
        stopListening();
        executor = executorFactory.get();
    }

    private void configureConnectionFactory(AMQPConnectionProperties properties) {
        connectionFactory.setUsername(properties.getUsername());
        connectionFactory.setPassword(properties.getPassword());
        connectionFactory.setVirtualHost(properties.getVirtualHost());
        connectionFactory.setConnectionTimeout(properties.getConnectionTimeout());
        connectionFactory.setRequestedHeartbeat(properties.getHeartbeatInterval());
        connectionFactory.setAutomaticRecoveryEnabled(properties.isAutomaticRecoveryEnabled());
    }

}
