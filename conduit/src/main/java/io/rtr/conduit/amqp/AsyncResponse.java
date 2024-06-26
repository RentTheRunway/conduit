package io.rtr.conduit.amqp;

/** Used to acknowledge multiple messages asynchronously */
public interface AsyncResponse {
    /** Responds to all previously unacknowledged messages, up to and including the given message */
    public void respondMultiple(AMQPMessageBundle messageBundle, ActionResponse actionResponse);

    /** Responds to a single unacknowledged message */
    public void respondSingle(AMQPMessageBundle messageBundle, ActionResponse actionResponse);
}
