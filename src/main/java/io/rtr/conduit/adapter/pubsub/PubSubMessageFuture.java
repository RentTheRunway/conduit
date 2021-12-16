package io.rtr.conduit.adapter.pubsub;

import com.google.api.core.ApiFuture;
import io.rtr.conduit.adapter.MessageFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubMessageFuture implements MessageFuture<String> {
//    private static final Logger log = LoggerFactory.getLogger(PubSubMessageFuture.class);

    private ApiFuture<String> future;

    PubSubMessageFuture(ApiFuture<String> future){
        this.future = future;
    }

    @Override
    public String get() {
        try {
            return this.future.get();
        }
        catch ( Exception e){
            System.err.println(e);
//            log.error("future get", e);
        }
        return null;
    }
}
