# Conduit

## What this be?

A simple abstraction over the rabbitmq java client, which hides most of the amqp/rabbitmq specific details.

## How do I publish?

    Publisher p = new Publisher(new AMQPPublishContext(
            username,
            password,
            exchange,
            routing-key,
            host,
            port
    ));
    p.connect();
    p.publish(new AMQPMessageBundle("hello-world");

## How do I consume?

    Consumer c = new Consumer(AMQPListenContext(
            username, 
            password,
            exchange,
            queue,
            host,
            port, 
            new AMQPConsumerCallback() {
                    @Override
                    void notifyOfActionFailure(Exception e) {}

                    @Override
                    Action handle(AMQPMessageBundle messageBundle) {

                        //! Message processing here

                        return Action.Acknowledge;
                    }
            }
    ));

## What versions of Java are supported

As of July 2019, Java 8 is supported.
Support for Java 6 and Java 7 has ended.

## Release process
When a PR is merged to master, a release will be made and deployed to Sonatype by Github Actions. This includes pushing new commits via `maven-release-plugin`. The released version will be automatically deployed and usable immediately after the `release` workflow completes.
