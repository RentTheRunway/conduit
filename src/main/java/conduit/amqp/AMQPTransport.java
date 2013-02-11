package conduit.amqp;

import com.rabbitmq.client.*;
import conduit.transport.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Currently, programmatic creation of exchanges and queues is disallowed and discouraged.
 *
 * User: kmandrika
 * Date: 1/8/13
 */
public class AMQPTransport extends Transport {
    private ConnectionFactory factory = new ConnectionFactory();
    private Connection connection;
    private Channel channel;

    protected static final Logger log = Logger.getLogger(AMQPTransport.class);

    public AMQPTransport(String host, int port) {
        factory.setHost(host);
        factory.setPort(port);
    }

    @Override
    protected void connectImpl(TransportConnectionProperties properties) throws IOException {
        AMQPConnectionProperties connectionProperties =
                (AMQPConnectionProperties)properties;

        factory.setUsername(connectionProperties.getUsername());
        factory.setPassword(connectionProperties.getPassword());
        factory.setVirtualHost(connectionProperties.getVirtualHost());
        factory.setConnectionTimeout(connectionProperties.getConnectionTimeout()); //! 0 is infinite.
        factory.setRequestedHeartbeat(connectionProperties.getHeartbeatInterval()); //! In seconds.

        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.basicQos(1);
    }

    @Override
    protected void closeImpl() throws IOException {
        //! We are going to assume that closing an already closed
        //  connection is considered success.
        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
            } catch (AlreadyClosedException ignored) {}
        }
    }

    private class AMQPQueueConsumer extends DefaultConsumer {
        private AMQPConsumerCallback callback;
        private int threshold;

        public AMQPQueueConsumer(Channel channel, AMQPConsumerCallback callback, int threshold) {
            super(channel);
            this.callback = callback;
            this.threshold = threshold;
        }

        @Override
        public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
            log.info("Shutdown handler invoked");
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            final long deliveryTag = envelope.getDeliveryTag();

            AMQPConsumerCallback.Action action;

            try {
                action = callback.handle(new AMQPMessageBundle(
                        consumerTag
                      , envelope
                      , properties
                      , body
                ));
            } catch (RuntimeException e) {
                //! Blanket exception handler for notifying, via the log, that the user-supplied
                //  callback let an exception propagate. In such cases, all the listeners are stopped.
                log.error("The user-supplied callback allowed an exception to propagate.");
                log.error("Catastrophic - all listeners have stopped! Exception: ", e);
                throw e;
            }

            //! We can't issue any blocking amqp calls in the context of this method, otherwise
            //  channel's internal thread(s) will deadlock. Both, basicAck and basicReject are
            //  asynchronous.

            try {
                if (action == AMQPConsumerCallback.Action.Acknowledge) {
                    channel.basicAck(deliveryTag, false);
                    return;
                }

                if (action == AMQPConsumerCallback.Action.RejectAndDiscard) {
                    log.warn("Discarding message, body = " + new String(body));
                    //! Let the broker know we rejected this message. Then publish this bad boy onto
                    //  the poison queue. Don't bother confirming for two reasons; we don't care, and
                    //  we can't issue blocking calls here.
                    publishToPoisonQueue(deliveryTag, envelope, properties, body);
                    return;
                }

                if (action == AMQPConsumerCallback.Action.RejectAndRequeue) {
                    log.warn("Received an unknown message, body = " + new String(body));
                    log.warn("\tAdjusting headers for retry.");

                    Map<String, Object> headers = properties.getHeaders();

                    //! If the message was published by the Conduit, this header should
                    //  exist.
                    int retry = Integer.parseInt(
                            headers.get("conduit-retry-count").toString()
                    );

                    //! Move to poison queue if threshold reached.
                    if (retry >= threshold) {
                        publishToPoisonQueue(deliveryTag, envelope, properties, body);
                        return;
                    }

                    //! Adjust headers and requeue.
                    headers.put("conduit-retry-count", retry + 1);
                    AMQP.BasicProperties retryProperties = new AMQP.BasicProperties()
                            .builder()
                            .type(properties.getType())
                            .deliveryMode(properties.getDeliveryMode())
                            .priority(properties.getPriority())
                            .headers(headers)
                            .build();

                    channel.basicReject(deliveryTag, false);
                    channel.basicPublish(
                            envelope.getExchange()
                          , envelope.getRoutingKey()
                          , retryProperties
                          , body
                    );
                }
            } catch (Exception e) {
                callback.notifyOfActionFailure(e);
            }
        }

        private void publishToPoisonQueue(long deliveryTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                throws IOException {
            channel.basicReject(deliveryTag, false);
            channel.basicPublish(
                    envelope.getExchange()
                  , envelope.getRoutingKey() + ".poison"
                  , properties
                  , body
            );
        }
    }

    @Override
    protected void listenImpl(TransportListenProperties properties) throws IOException {
        final boolean noAutoAck = false;

        AMQPListenProperties listenProperties =
                (AMQPListenProperties)properties;
        AMQPQueueConsumer consumer =
                new AMQPQueueConsumer(channel, listenProperties.getCallback(), listenProperties.getThreshold());

        channel.basicConsume(listenProperties.getQueue(), noAutoAck, consumer);
    }

    @Override
    protected void stopImpl() throws IOException {
        //! As with closing the connection, closing an already
        //  closed channel is considered success.
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (AlreadyClosedException ignored) {}
        }
    }

    @Override
    protected boolean publishImpl(TransportMessageBundle bundle, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        AMQPPublishProperties publishProperties = (AMQPPublishProperties)properties;
        AMQPMessageBundle messageBundle = (AMQPMessageBundle)bundle;

        channel.basicPublish(
                publishProperties.getExchange()
              , publishProperties.getRoutingKey()
              , messageBundle.getBasicProperties()
              , messageBundle.getBody()
        );

        return channel.waitForConfirms(publishProperties.getTimeout());
    }

    @Override
    protected <E> boolean transactionalPublishImpl(Collection<E> messageBundles, TransportPublishProperties properties)
            throws IOException, TimeoutException, InterruptedException {
        channel.txSelect();

        boolean rollback = true;

        try {
            for (E messageBundle : messageBundles) {
                if (!publishImpl((AMQPMessageBundle)messageBundle, properties))
                    return false;
            }
            rollback = false;
        } finally {
            //! Explicitly roll back.
            if (rollback)
                channel.txRollback();
            else
                channel.txCommit();
        }

        return !rollback;
    }
}
