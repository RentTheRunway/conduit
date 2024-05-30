# Conduit

A simple abstraction over the RabbitMQ Java client, which hides most of the AMQP-specific details.

## How do I publish?

```java
final Publisher publisher = new Publisher(new AMQPPublishContext(
        username,
        password,
        exchange,
        routingKey,
        host,
        port));
publisher.connect();
publisher.publish(new AMQPMessageBundle("hello-world"));
```

## How do I consume?

```java
final Consumer consumer = new Consumer(new AMQPListenContext(
        username,
        password,
        exchange,
        queue,
        host,
        port,
        new AMQPConsumerCallback() {
                @Override
                void notifyOfActionFailure(final Exception e) {}

                @Override
                Action handle(final AMQPMessageBundle messageBundle) {
                    // message processing goes here

                    return Action.Acknowledge;
                }
        }));
```

## What versions of Java are supported?

Java 8 and higher are supported. The build uses Java 21 and targets 8.

## Release process

Manually run the Release workflow in GitHub Actions.
