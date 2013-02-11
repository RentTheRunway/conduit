package conduit.amqp;

/**
 * Interface for the callback invoked by the transport consumer on a new message
 * arrival. The _Actions_ enumeration will dictate to the framework what to do. More
 * specifically, the framework will acknowledge the message explicitly if _Acknowledge_ is returned.
 * In the case of a framework action failure, the _notifyOfActionFailure_ callback will be issued with
 * an appropriate exception. An action failure could happen if the broker goes down or the
 * queue/exchange is manually deleted.
 *
 * User: kmandrika
 * Date: 1/8/13
 */
public interface AMQPConsumerCallback {
    //! An action which the transport should take after returning from
    //  the handle callback.
    public enum Action {
            Acknowledge         //! The transport will ack the message explicitly.
          , RejectAndRequeue    //! The message wasn't meant to be processed.
                                //  For example, if the message delivered is of
                                //  a higher version than what we are able to
                                //  deal with.
          , RejectAndDiscard    //! A malformed message, place it on a poison queue.
    }

    Action handle(AMQPMessageBundle messageBundle);
    void notifyOfActionFailure(Exception e);
}
