package org.hormigas.ws.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Dead-letter retract-sweep config (Strategy C, ADR-014). The sweep cadence is read by {@code @Scheduled}
 * as {@code processing.deadletter.cleanup-every}.
 */
@ConfigMapping(prefix = "processing.deadletter")
public interface DeadLetterConfig {

    /** Confirmed-set drain page size per sweep. ↑ retracts drafts faster, heavier Redis/DB per tick; ↓ gentler. */
    @WithDefault("500")
    int cleanupBatch();
}
