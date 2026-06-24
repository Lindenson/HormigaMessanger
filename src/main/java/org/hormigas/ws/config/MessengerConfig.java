package org.hormigas.ws.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

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

        PersistBatch persistBatch();

        /**
         * Group-commit of the inbound persist (plan B). The reactive inbound pipeline runs in
         * PARALLEL mode so many {@code routeIn} flows are in flight at once; this accumulator
         * coalesces their {@code history+outbox} writes into one transaction per batch, and runs
         * up to {@code maxConcurrentBatches} such transactions concurrently to fill the otherwise
         * ~95%-idle DB pool. Defaults sized for a 20-connection pool (see load-test findings R2).
         */
        interface PersistBatch {
            /** Max messages coalesced into a single transaction. */
            @WithDefault("64")
            int maxSize();

            /** Max time (ms) a partial batch waits before flushing — bounds the added latency. */
            @WithDefault("5")
            int lingerMs();

            /** Max batch transactions in flight at once (keep ≤ DB pool size). */
            @WithDefault("8")
            int maxConcurrentBatches();
        }
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
        int maxMs();
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
