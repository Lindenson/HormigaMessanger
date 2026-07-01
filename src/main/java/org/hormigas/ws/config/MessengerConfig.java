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
    ConversationCache conversationCache();
    ReadBatch readBatch();
    StreamRetry streamRetry();

    /**
     * Re-subscribe backoff shared by the long-lived reactive streams (inbound/outbound publishers and the
     * group-commit batchers): on a terminal stream failure they retry indefinitely with this exponential
     * backoff so a transient fault never permanently disables them.
     */
    interface StreamRetry {
        /** Initial re-subscribe delay. */
        @WithDefault("200")
        int minBackoffMs();

        /** Max re-subscribe delay (exponential cap). */
        @WithDefault("5000")
        int maxBackoffMs();
    }

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
        String everyS();
    }

    interface Storage {
        String service();
    }

    /**
     * In-process L1 conversation cache (Phase 2, 4.1). Declared here so the strict
     * {@code processing.messages} mapping accepts these keys; {@code CachedConversationDirectory}
     * reads the same canonical keys directly. Defaults match that adapter's.
     */
    interface ConversationCache {
        @WithDefault("100000")
        long maxSize();

        @WithDefault("60")
        int ttlSeconds();
    }

    /**
     * Group-commit of read-status writes (READ_IN), mirroring the inbound persist batcher: READ receipts
     * hit the DB only in batches, never row-per-event. Same shape/knobs as {@code inbound.persist-batch}.
     */
    interface ReadBatch {
        @WithDefault("64")
        int maxSize();

        @WithDefault("10")
        int lingerMs();

        @WithDefault("4")
        int maxConcurrentBatches();
    }

    interface Idempotent {
        int ttlSeconds();
    }
}
