package org.hormigas.ws.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * History retention config (UC-U23, FR-RET-04/05). Two classes: normal history vs (longer-lived) frozen
 * contract records. The sweep cadence is read by {@code @Scheduled} as {@code processing.retention.every}.
 */
@ConfigMapping(prefix = "processing.retention")
public interface RetentionConfig {

    /** Age after which normal history rows are purged (frozen rows are excluded from this sweep). */
    @WithDefault("90")
    int historyDays();

    /** Longer TTL for frozen (contract) messages. Must be ≥ {@link #historyDays()} to be meaningful. */
    @WithDefault("365")
    int frozenDays();
}
