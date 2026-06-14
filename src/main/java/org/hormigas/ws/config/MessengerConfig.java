package org.hormigas.ws.config;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "processing.messages")
public interface MessengerConfig {

    Inbound inbound();
    Outbound outbound();
    Feedback feedback();
    Channel channel();
    Credits credits();
    Collector collector();
    Storage storage();
    Idempotent idempotent();

    interface Inbound {
        int queueSize();
    }

    interface Outbound {
        int batchSize();
        int queueSize();
        String pollingMs();
    }

    interface Feedback {
        int additionalMs();
        double adjustmentFactor();
        double recoveryFactor();
    }

    interface Channel {
        boolean retry();
        int minBackoffMs();
        int maxBackoffMs();
        int maxRetries();
    }

    interface Credits {
        int maxValue();
        int refillRatePerS();
    }

    interface Collector {
        int maxWatermarks();
        String everyS();
    }

    interface Storage {
        String service();
    }

    interface Idempotent {
        int ttlSeconds();
    }
}
