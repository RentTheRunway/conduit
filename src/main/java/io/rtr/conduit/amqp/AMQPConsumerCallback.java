package io.rtr.conduit.amqp;

import com.rabbitmq.client.ShutdownSignalException;

/**
 * Interface for the callback invoked by the transport consumer on a new message
 * arrival. The _Actions_ enumeration will dictate to the framework what to do. More
 * specifically, the framework will acknowledge the message explicitly if _Acknowledge_ is returned.
 * In the case of a framework action failure, the _notifyOfActionFailure_ callback will be issued with
 * an appropriate exception. An action failure could happen if the broker goes down or the
 * queue/exchange is manually deleted.
 */
public interface AMQPConsumerCallback {
    Action handle(AMQPMessageBundle messageBundle);
    void notifyOfActionFailure(Exception e);
    void notifyOfShutdown(String consumerTag, ShutdownSignalException sig);
}
