package io.rtr.conduit.adapter.ampq;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import io.rtr.conduit.adapter.PublisherBuilder;
import io.rtr.conduit.adapter.pubsub.PubSubPublisherWrapper;
import io.rtr.conduit.amqp.impl.AMQPPublisherBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class AmpqBuilder implements PublisherBuilder {
//    private static final Logger log = LoggerFactory.getLogger(AmpqBuilder.class);
    private String host;
    private String port;
    private String username;
    private String password;
    private String exchange;
    private String routingKey;


    private Publisher publisher;

    protected AmpqBuilder() {
    }

    public static AmpqBuilder builder() {
        return new AmpqBuilder();
    }

    @Override
    public AmpqBuilder withProperties(Properties properties) {
        this.host = properties.getProperty("host");
        this.port = properties.getProperty("port");
        this.username = properties.getProperty("username");
        this.password = properties.getProperty("password");
        this.exchange = properties.getProperty("exchange");
        this.routingKey = properties.getProperty("routingKey");
        return this;
    }

    public final io.rtr.conduit.adapter.Publisher build(){
        try {
            final io.rtr.conduit.amqp.publisher.Publisher publisher = AMQPPublisherBuilder.builder()
                    .host(this.host)
                    .port(Integer.parseInt(this.port))
                    .username(this.username)
                    .password(this.password)
                    .exchange(this.exchange)
                    .routingKey(this.routingKey)
                    .build();
            publisher.connect();
            return new AmpqPublisherWrapper(publisher);
        } catch ( Exception e ){
            System.err.println(e);
//            log.error("build error",e);
        }
        return null;
    }
}
