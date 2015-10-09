# Conduit

## What this be?

A simple abstraction over the rabbitmq java client, which hides most of the amqp/rabbitmq specific details.

## How do I publish?

    Publisher p = new Publisher(new AMQPPublishContext(
            username
          , password
          , exchange
          , routing-key
          , host
          , port
    ));
    p.connect();
    p.publish(new AMQPMessageBundle("hello-world");

## How do I consume?

    Consumer c = new Consumer(AMQPListenContext(
            username
          , password
          , exchange
          , queue
          , host
          , port
          , new AMQPConsumerCallback() {
                    @Override
                    void notifyOfActionFailure(Exception e) {}

                    @Override
                    Action handle(AMQPMessageBundle messageBundle) {

                        //! Message processing here

                        return Action.Acknowledge;
                    }
            }
    ));

## What's next?

If we can hit that bullseye, the rest of the dominoes will fall like a house of cards, checkmate!
