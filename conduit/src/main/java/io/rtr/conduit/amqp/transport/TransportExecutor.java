package io.rtr.conduit.amqp.transport;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportExecutor extends ThreadPoolExecutor {

    // From ConsumerWorkService
    private static final int DEFAULT_NUM_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    private static ThreadFactory buildCountedDaemonThreadFactory() {
        AtomicInteger threadCount = new AtomicInteger(0);
        return run -> {
            Thread thread = new Thread(run);
            thread.setDaemon(true);
            thread.setName(String.format("AMQPConnection-%s", threadCount.getAndIncrement()));
            return thread;
        };
    }

    public TransportExecutor() {
        this(DEFAULT_NUM_THREADS, buildCountedDaemonThreadFactory());
    }

    /**
     * Based on Executors.newFixedThreadPool
     *
     * @param nThreads
     * @param threadFactory
     */
    public TransportExecutor(int nThreads, ThreadFactory threadFactory) {
        super(
                nThreads,
                nThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                threadFactory);
    }
}
