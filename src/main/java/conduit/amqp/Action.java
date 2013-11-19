package conduit.amqp;

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
