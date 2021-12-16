package io.rtr.conduit.adapter;

import io.rtr.conduit.adapter.pubsub.PubSubBuilder;

import java.util.Properties;

public class PublisherAdapterFactory {

    private static volatile PublisherAdapterFactory instance;

    public static PublisherAdapterFactory getInstance() {
        if (instance == null) {
            synchronized (PublisherAdapterFactory.class) {
                if (instance == null) {
                    instance = new PublisherAdapterFactory();
                }
            }
        }
        return instance;
    }

    PublisherBuilder getBuilder(String name){
        if ( name.equalsIgnoreCase("PubSub") ){
            return PubSubBuilder.builder();
        }
        return null;
    }

    public static void main(String[] args){
        final PublisherAdapterFactory factory = PublisherAdapterFactory.getInstance();
        final PublisherBuilder builder = factory.getBuilder("PubSub");
        final Properties properties = new Properties();
        properties.setProperty("topicId","killthewabbit");
        properties.setProperty("projectId","kill-the-wabbit-335017");
        properties.setProperty("credentialsResource","/Users/since/work/RentTheRunway/KillTheWabbit/kill-the-wabbit.json");
        builder.withProperties(properties);
        final Publisher publisher = builder.build();
        final MessageFuture future = publisher.publish("Hello World");
        System.out.println("message id: " + future.get());
    }
}
