package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.rtr.conduit.amqp.AMQPMessageBundle;
import io.rtr.conduit.amqp.publisher.Publisher;
import org.easymock.IAnswer;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;

import static org.easymock.EasyMock.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        AMQPTransport.class
      , AMQPPublishContext.class
      , ConnectionFactory.class
      , Connection.class
      , Channel.class
})
public class PublisherTest {

    public static final String DEFAULT_ROUTING_KEY = "routingKey";
    public static final String DEFAULT_EXCHANGE = "exchange";

    private class AMQPMockedTransportInternals {
        public ConnectionFactory connectionFactoryMock;
        public Connection connectionMock;
        public Channel channelMock;
    }

    AMQPMockedTransportInternals mock() throws Exception {
        AMQPMockedTransportInternals mocked = new AMQPMockedTransportInternals();

        mocked.connectionFactoryMock = PowerMock.createPartialMock(ConnectionFactory.class, "newConnection");
        mocked.connectionMock = PowerMock.createPartialMock(Connection.class, "createChannel");
        mocked.channelMock = PowerMock.createPartialMock(
                Channel.class
              , "basicAck"
              , "basicReject"
              , "basicPublish"
              , "basicConsume"
              , "basicPublish"
              , "waitForConfirms"
              , "txSelect"
              , "txRollback"
              , "txCommit"
        );

        PowerMock.suppress(PowerMock.methods(
                ConnectionFactory.class
              , "setHost"
              , "setPort"
              , "setUsername"
              , "setPassword"
              , "setVirtualHost"
              , "setConnectionTimeout"
              , "setRequestedHeartbeat"
        ));
        PowerMock.expectPrivate(
                mocked.channelMock
              , "basicQos"
              , anyInt()
        ).andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    return null;
                }
        });

        PowerMock.expectNew(ConnectionFactory.class).andReturn(mocked.connectionFactoryMock);
        PowerMock.expectPrivate(mocked.connectionFactoryMock, "newConnection").andReturn(mocked.connectionMock);
        PowerMock.expectPrivate(mocked.connectionMock, "createChannel").andReturn(mocked.channelMock);

        return mocked;
    }

    @Test
    public void testAMQPPublishDeep() throws Exception {
        final ArrayList<String> published = new ArrayList<String>();

        AMQPMockedTransportInternals mocked = mock();

        PowerMock.expectPrivate(mocked.channelMock, "confirmSelect").andReturn(null);
        PowerMock.expectPrivate(mocked.channelMock, "waitForConfirms", anyLong()).andReturn(true);
        PowerMock.expectPrivate(
                mocked.channelMock
              , "basicPublish"
              , eq(DEFAULT_EXCHANGE)
              , eq(DEFAULT_ROUTING_KEY)
              , anyObject(AMQP.BasicProperties.class)
              , anyObject(byte[].class)
        ).andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    published.add(new String((byte[])getCurrentArguments()[3]));
                    return null;
                }
        });
        PowerMock.replayAll();

        Publisher publisher = getPublisher();
        publisher.connect();

        assertTrue(publisher.publish(new AMQPMessageBundle("hello")));
        assertTrue(published.size() == 1);
        assertEquals(published.get(0), "hello");
    }

    @Test
    public void testAMQPPublishDeepNegative() throws Exception {
        AMQPMockedTransportInternals mocked = mock();

        PowerMock.expectPrivate(mocked.channelMock, "confirmSelect").andReturn(null);
        PowerMock.expectPrivate(mocked.channelMock, "waitForConfirms", anyLong()).andReturn(false);
        PowerMock.expectPrivate(
                mocked.channelMock
              , "basicPublish"
              , eq(DEFAULT_EXCHANGE)
              , eq(DEFAULT_ROUTING_KEY)
              , anyObject(AMQP.BasicProperties.class)
              , anyObject(byte[].class)
        ).andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    return null;
                }
        });
        PowerMock.replayAll();

        Publisher publisher = getPublisher();
        publisher.connect();

        assertFalse(publisher.publish(new AMQPMessageBundle("hello")));
    }

    @Test
    public void testAMQPPublishDeepOverridePublishProperties() throws Exception {
        final ArrayList<String> published = new ArrayList<String>();

        AMQPMockedTransportInternals mocked = mock();
        String overriddenExchange = "overriddenExchange";
        String overriddenRoutingKey = "overriddenRoutingKey";
        AMQPPublishProperties publishProperties = new AMQPPublishProperties(overriddenExchange, overriddenRoutingKey);

        PowerMock.expectPrivate(mocked.channelMock, "confirmSelect").andReturn(null);
        PowerMock.expectPrivate(mocked.channelMock, "waitForConfirms", anyLong()).andReturn(true);
        PowerMock.expectPrivate(
            mocked.channelMock
            , "basicPublish"
            , eq(overriddenExchange)
            , eq(overriddenRoutingKey)
            , anyObject(AMQP.BasicProperties.class)
            , anyObject(byte[].class)
        ).andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                published.add(new String((byte[])getCurrentArguments()[3]));
                return null;
            }
        });
        PowerMock.replayAll();

        Publisher publisher = getPublisher();
        publisher.connect();

        assertTrue(publisher.publish(new AMQPMessageBundle("hello"), publishProperties));
        assertTrue(published.size() == 1);
        assertEquals(published.get(0), "hello");
    }

    private Publisher getPublisher() {
        return AMQPPublisherBuilder.builder()
                    .host("host")
                    .username("username")
                    .password("password")
                    .exchange(DEFAULT_EXCHANGE)
                    .routingKey(DEFAULT_ROUTING_KEY)
                    .port(5672)
                    .confirmEnabled(true)
                    .build();
    }

    private class BooleanWrapper {
        private boolean value = false;
        public void set() { value = true; }
        public boolean get() { return value; }
    }

    @Test
    public void testAMQPTransactionalPublishDeep() throws Exception {
        final ArrayList<String> published = new ArrayList<String>();

        AMQPMockedTransportInternals mocked = mock();

        final BooleanWrapper txSelectCalled = new BooleanWrapper();
        final BooleanWrapper txCommitCalled = new BooleanWrapper();

        PowerMock.expectPrivate(mocked.channelMock, "confirmSelect").andReturn(null).anyTimes();
        PowerMock.expectPrivate(mocked.channelMock, "waitForConfirms", anyLong()).andReturn(true).anyTimes();
        PowerMock.expectPrivate(mocked.channelMock, "txSelect").andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    txSelectCalled.set();
                    return null;
                }
        });
        PowerMock.expectPrivate(mocked.channelMock, "txCommit").andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    txCommitCalled.set();
                    return null;
                }
        });
        PowerMock.expectPrivate(
                mocked.channelMock
              , "basicPublish"
              , anyObject(String.class)
              , anyObject(String.class)
              , anyObject(AMQP.BasicProperties.class)
              , anyObject(byte[].class)
        ).andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    published.add(new String((byte[])getCurrentArguments()[3]));
                    return null;
                }
        }).anyTimes();
        PowerMock.replayAll();

        Publisher publisher = getPublisher();
        publisher.connect();

        AMQPMessageBundle[] messageBundles = {
                new AMQPMessageBundle("hello")
              , new AMQPMessageBundle("world")
        };

        assertTrue(publisher.transactionalPublish(Arrays.asList(messageBundles)));
        assertTrue(txSelectCalled.get());
        assertTrue(txCommitCalled.get());
        assertTrue(published.get(0).equals("hello"));
        assertTrue(published.get(1).equals("world"));
    }

    @Test
    public void testAMQPTransactionalPublishDeepNegative() throws Exception {
        final ArrayList<String> published = new ArrayList<String>();

        AMQPMockedTransportInternals mocked = mock();

        final BooleanWrapper txSelectCalled = new BooleanWrapper();
        final BooleanWrapper txRollbackCalled = new BooleanWrapper();

        PowerMock.expectPrivate(mocked.channelMock, "confirmSelect").andReturn(null);
        PowerMock.expectPrivate(mocked.channelMock, "waitForConfirms", anyLong()).andReturn(false).anyTimes();
        PowerMock.expectPrivate(mocked.channelMock, "txSelect").andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    txSelectCalled.set();
                    return null;
                }
        });
        PowerMock.expectPrivate(mocked.channelMock, "txRollback").andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    txRollbackCalled.set();
                    return null;
                }
        });
        PowerMock.expectPrivate(
                mocked.channelMock
              , "basicPublish"
              , anyObject(String.class)
              , anyObject(String.class)
              , anyObject(AMQP.BasicProperties.class)
              , anyObject(byte[].class)
        ).andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    published.add(new String((byte[])getCurrentArguments()[3]));
                    return null;
                }
        }).anyTimes();
        PowerMock.replayAll();

        Publisher publisher = getPublisher();
        publisher.connect();

        AMQPMessageBundle[] messageBundles = {
                new AMQPMessageBundle("hello")
              , new AMQPMessageBundle("world")
        };

        assertFalse(publisher.transactionalPublish(Arrays.asList(messageBundles)));
        assertFalse(published.size() == 0);
        assertTrue(txSelectCalled.get());
        assertTrue(txRollbackCalled.get());
    }
}
