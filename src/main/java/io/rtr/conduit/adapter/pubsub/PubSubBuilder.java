package io.rtr.conduit.adapter.pubsub;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import io.rtr.conduit.adapter.PublisherBuilder;
import io.rtr.conduit.amqp.impl.AMQPAsyncQueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PubSubBuilder implements PublisherBuilder {
//    private static final Logger log = LoggerFactory.getLogger(PubSubBuilder.class);
    private String topicId;
    private String projectId;
    private InputStream credentialsStream;
    private String credentialsResource;

    private Publisher publisher;

    protected PubSubBuilder() {
    }

    public static PubSubBuilder builder() {
        return new PubSubBuilder();
    }

    public PubSubBuilder withProjectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public PubSubBuilder withTopicId(String topicId) {
        this.topicId = topicId;
        return this;
    }

    public PubSubBuilder withCredentialsStream(InputStream credentialsStream) {
        this.credentialsStream = credentialsStream;
        return this;
    }

    @Override
    public PubSubBuilder withProperties(Properties properties) {
        if ( properties.containsKey("topicId") ) {
            this.topicId = properties.getProperty("topicId");
        }
        if ( properties.containsKey("projectId")  ){
            this.projectId = properties.getProperty("projectId");
        }
        if ( properties.containsKey("credentialsResource" ) ){
            this.credentialsResource = properties.getProperty("credentialsResource");
        }
        return this;
    }

    public final io.rtr.conduit.adapter.Publisher build(){
        try {
            final InputStream credentialsInputStream = this.credentialsResource != null
                    ? new FileInputStream(this.credentialsResource)
                    : credentialsStream;
            final GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsInputStream);
            final TopicName topicName = TopicName.of(projectId, topicId);
            final Publisher publisherImpl = Publisher.newBuilder(topicName)
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .build();
            return new PubSubPublisherWrapper(publisherImpl);
        } catch ( Exception e ){
            System.err.println(e);
//            log.error("build error",e);
        }
        return null;
    }
}
