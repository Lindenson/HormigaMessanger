package org.hormigas.ws.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Session reaper config (FR-PRES-03 liveness + T2 overload). The reaper cadences themselves are read by
 * {@code @Scheduled} as {@code processing.session.reaper-every} / {@code .overload-reaper-every}.
 */
@ConfigMapping(prefix = "processing.session")
public interface SessionConfig {

    /** Idle cutoff — a connection silent this long (missed pings) is reaped. ↑ tolerates flaky links; ↓ frees phantom-online sooner. */
    @WithDefault("35000")
    long idleTimeoutMs();

    /** Max un-ACKed backlog per recipient before force-disconnect (caps the pending ZSET/outbox growth). */
    @WithDefault("1000")
    int maxPending();

    /** Max heavy clients kicked per overload sweep. ↑ clears backlog faster (burstier); ↓ smooths the reaper. */
    @WithDefault("100")
    int overloadKickBatch();

    /** Liveness (idle) sweep cadence — Duration string. Also read by {@code @Scheduled(every=...)}. */
    @WithDefault("15s")
    String reaperEvery();

    /** Overload (backlog) sweep cadence — Duration string. Also read by {@code @Scheduled(every=...)}. */
    @WithDefault("20s")
    String overloadReaperEvery();
}
