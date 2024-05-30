package io.rtr.conduit.amqp;

import com.rabbitmq.client.ShutdownSignalException;

/**
 * Interface for the callback invoked by the transport consumer on a new message arrival. The
 * consumer has the option of responding to any message immediately or at a later time using the
 * given _ActionResponse_ object. In the case of a framework action failure, the
 * _notifyOfActionFailure_ callback will be issued with an appropriate exception. An action failure
 * could happen if the broker goes down or the queue/exchange is manually deleted.
 */
public interface AMQPAsyncConsumerCallback {
    void handle(AMQPMessageBundle messageBundle, AsyncResponse response);

    void notifyOfActionFailure(Exception e);

    void notifyOfShutdown(String consumerTag, ShutdownSignalException sig);
}
