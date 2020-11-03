package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MetricsCollector;
import io.rtr.conduit.amqp.transport.TransportExecutor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AMQPConnectionTest {
    private final static int CONNECTION_TIMEOUT = 1337;
    private final static int PORT = 42;

    Connection mockConnection;
    ConnectionFactory mockFactory;
    TransportExecutor mockExecutor;
    MetricsCollector mockMetrics;

    private AMQPConnection defaultTestConnection() {

        return new AMQPConnection(mockFactory, ()->mockExecutor, false, "RABBIT HOST", PORT, null);
    }

    private AMQPConnectionProperties defaultTestConnectionProps() {
        return new AMQPConnectionProperties(
                "BOB",
                "BOBS PASSWORD",
                "BOBS VHOST",
                CONNECTION_TIMEOUT,
                666,
                true);
    }

    @Before
    public void before() throws IOException, TimeoutException {
        mockFactory = mock(ConnectionFactory.class);
        mockExecutor = mock(TransportExecutor.class);
        mockMetrics = mock(MetricsCollector.class);

        mockConnection = mock(Connection.class);
        when(mockConnection.createChannel()).thenReturn(mock(Channel.class));
        when(mockConnection.isOpen()).thenReturn(true);
        doAnswer((i)->when(mockConnection.isOpen())
                .thenReturn(false))
                .when(mockConnection).close(anyInt());
        when(mockFactory.newConnection(mockExecutor)).thenReturn(mockConnection);
        when(mockFactory.getConnectionTimeout()).thenReturn(CONNECTION_TIMEOUT);
    }

    @Test
    public void testConstructor_NoSll_SetsHostPortAndMetrics() throws Exception {
        new AMQPConnection(mockFactory, ()->mockExecutor, false, "RABBIT HOST", PORT, mockMetrics);
        verify(mockFactory).setHost("RABBIT HOST");
        verify(mockFactory).setPort(PORT);
        verify(mockFactory).setMetricsCollector(mockMetrics);
        verify(mockFactory, never()).setSocketFactory(any());

    }

    @Test
    public void testConstructor_Sll_SetsSocketFactory() throws Exception {
        new AMQPConnection(mockFactory, ()->mockExecutor, true, "RABBIT HOST", PORT, mockMetrics);
        verify(mockFactory).setHost("RABBIT HOST");
        verify(mockFactory).setPort(PORT);
        verify(mockFactory).setMetricsCollector(mockMetrics);
        verify(mockFactory).setSocketFactory(any());

    }

    @Test
    public void testConstructor_MetricsNull_SetsNoMetrics() throws Exception {
        new AMQPConnection(mockFactory, ()->mockExecutor, false, "RABBIT HOST", PORT, null);
        verify(mockFactory, never()).setMetricsCollector(any());
    }

    @Test
    public void testConnect_NotConnected_TransfersPropsToFactoryAndConnects() throws IOException, TimeoutException {
        defaultTestConnection().connect(defaultTestConnectionProps());

        verify(mockFactory).setUsername("BOB");
        verify(mockFactory).setPassword("BOBS PASSWORD");
        verify(mockFactory).setVirtualHost("BOBS VHOST");
        verify(mockFactory).setConnectionTimeout(CONNECTION_TIMEOUT);
        verify(mockFactory).setRequestedHeartbeat(666);
        verify(mockFactory).setAutomaticRecoveryEnabled(true);

        verify(mockFactory).newConnection(mockExecutor);
    }

    @Test
    public void testConnect_AlreadyConnected_DoesNothing() throws IOException, TimeoutException {
        AMQPConnection conn = defaultTestConnection();
        conn.connect(defaultTestConnectionProps());
        conn.connect(defaultTestConnectionProps());

        verify(mockFactory, times(1)).newConnection(mockExecutor);
    }

    @Test
    public void testDisconnect_Connected_ClosesConnection() throws IOException, TimeoutException {
        AMQPConnection conn = defaultTestConnection();
        conn.connect(defaultTestConnectionProps());
        conn.disconnect();

        verify(mockConnection).close(CONNECTION_TIMEOUT);
        verify(mockExecutor).shutdown();
    }

    @Test
    public void testDisconnect_NotConnected_DoesNothing() throws IOException, TimeoutException {
        defaultTestConnection().disconnect();
        verify(mockConnection, never()).close(anyInt());
        verify(mockExecutor, never()).shutdown();
    }

    @Test
    public void testDisconnect_AlreadyDisconnected_DoesNothing() throws IOException, TimeoutException {
        AMQPConnection conn = defaultTestConnection();
        conn.connect(defaultTestConnectionProps());
        conn.disconnect();
        conn.disconnect();

        verify(mockConnection, times(1)).close(CONNECTION_TIMEOUT);
        verify(mockExecutor, times(1)).shutdown();
    }

    @Test
    public void testStopListening_Connected_OnlyShutsDownExecutor() throws IOException, TimeoutException {
        AMQPConnection conn = defaultTestConnection();
        conn.connect(defaultTestConnectionProps());
        conn.stopListening();

        verify(mockConnection, never()).close(CONNECTION_TIMEOUT);
        verify(mockExecutor).shutdown();
    }

    @Test
    public void testStopListening_NotConnected_DoesNothing() {
        defaultTestConnection().stopListening();
        verify(mockExecutor, never()).shutdown();
    }

    @Test
    public void testStopListening_MultipleCalls_OnlyShutsDownExecutorOnce() throws IOException, TimeoutException {
        AMQPConnection conn = defaultTestConnection();
        conn.connect(defaultTestConnectionProps());
        conn.stopListening();
        conn.stopListening();
        conn.stopListening();
        conn.stopListening();

        verify(mockConnection, never()).close(CONNECTION_TIMEOUT);
        verify(mockExecutor, times(1)).shutdown();
    }

    @Test
    public void testCreateChannel_Connected_CreatesQos1Channel() throws IOException, TimeoutException {
        AMQPConnection conn = defaultTestConnection();
        conn.connect(defaultTestConnectionProps());
        Channel channel = conn.createChannel();
        verify(channel).basicQos(1);
    }

    @Test
    public void testCreateChannel_NotConnected_Throws() {
        AMQPConnection conn = defaultTestConnection();

        Assert.assertThrows(IllegalStateException.class, conn::createChannel);
    }

    @Test
    public void testIsConnected_Connected_ReturnsTrue() throws IOException, TimeoutException {
        AMQPConnection conn = defaultTestConnection();
        Assert.assertFalse(conn.isConnected());
        conn.connect(defaultTestConnectionProps());
        Assert.assertTrue(conn.isConnected());
        conn.disconnect();
        Assert.assertFalse(conn.isConnected());

        Assert.assertThrows(IllegalStateException.class, conn::createChannel);
    }

    @Test
    public void testWaitToStopListening_Connected_CallsAwaitTerminationOnExecutor() throws IOException, TimeoutException, InterruptedException {
        AMQPConnection conn = defaultTestConnection();
        conn.connect(defaultTestConnectionProps());
        when(mockExecutor.awaitTermination(1338, TimeUnit.MILLISECONDS)).thenReturn(true);
        Assert.assertTrue(conn.waitToStopListening(1338));
        verify(mockExecutor).awaitTermination(1338, TimeUnit.MILLISECONDS);
        when(mockExecutor.awaitTermination(1338, TimeUnit.MILLISECONDS)).thenReturn(false);
        Assert.assertFalse(conn.waitToStopListening(1338));
        verify(mockExecutor, times(2)).awaitTermination(1338, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testWaitToStopListening_NotConnected_ReturnsTrue() throws IOException, TimeoutException, InterruptedException {
        Assert.assertTrue(defaultTestConnection().waitToStopListening(1338));
        verify(mockExecutor, never()).awaitTermination(1338, TimeUnit.MILLISECONDS);
    }
}
