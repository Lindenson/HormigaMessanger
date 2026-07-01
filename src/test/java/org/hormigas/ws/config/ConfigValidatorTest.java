package org.hormigas.ws.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("ConfigValidator — fail-fast semantic validation of the runtime config")
class ConfigValidatorTest {

    /** A validator wired with all-valid mocked configs; individual tests then break one knob. */
    private ConfigValidator validValidator() {
        ConfigValidator v = new ConfigValidator();
        v.messenger = validMessenger();
        v.rateLimit = validRateLimit();
        v.attachments = validAttachments();
        v.minio = validMinio();
        v.session = validSession();
        v.retention = validRetention();
        v.deadLetter = validDeadLetter();
        v.dbPoolSize = 20;
        return v;
    }

    @Test
    @DisplayName("a fully valid configuration passes without throwing")
    void validConfigPasses() {
        assertDoesNotThrow(() -> validValidator().validate());
    }

    @Test
    @DisplayName("max-backoff < min-backoff is rejected")
    void streamRetryOrderingEnforced() {
        ConfigValidator v = validValidator();
        MessengerConfig.StreamRetry sr = mock(MessengerConfig.StreamRetry.class);
        when(sr.minBackoffMs()).thenReturn(5000);
        when(sr.maxBackoffMs()).thenReturn(200);
        when(v.messenger.streamRetry()).thenReturn(sr);

        IllegalStateException ex = assertThrows(IllegalStateException.class, v::validate);
        assertTrue(ex.getMessage().contains("stream-retry.max-backoff-ms"), ex.getMessage());
    }

    @Test
    @DisplayName("adjustment-factor ≤ 1 and recovery-factor ≥ 1 are rejected")
    void feedbackFactorsEnforced() {
        ConfigValidator v = validValidator();
        MessengerConfig.Feedback fb = mock(MessengerConfig.Feedback.class);
        when(fb.additionalMs()).thenReturn(1000);
        when(fb.maxMs()).thenReturn(30000);
        when(fb.adjustmentFactor()).thenReturn(1.0);   // must be > 1
        when(fb.recoveryFactor()).thenReturn(1.5);     // must be in (0,1)
        when(v.messenger.feedback()).thenReturn(fb);

        IllegalStateException ex = assertThrows(IllegalStateException.class, v::validate);
        assertTrue(ex.getMessage().contains("adjustment-factor"), ex.getMessage());
        assertTrue(ex.getMessage().contains("recovery-factor"), ex.getMessage());
    }

    @Test
    @DisplayName("batch concurrency above the DB pool size is rejected")
    void batchConcurrencyBoundedByPool() {
        ConfigValidator v = validValidator();
        v.dbPoolSize = 4;
        MessengerConfig.Inbound.PersistBatch pb = mock(MessengerConfig.Inbound.PersistBatch.class);
        when(pb.maxSize()).thenReturn(64);
        when(pb.lingerMs()).thenReturn(5);
        when(pb.maxConcurrentBatches()).thenReturn(8);  // > pool 4
        when(v.messenger.inbound().persistBatch()).thenReturn(pb);

        IllegalStateException ex = assertThrows(IllegalStateException.class, v::validate);
        assertTrue(ex.getMessage().contains("persist-batch.max-concurrent-batches"), ex.getMessage());
    }

    @Test
    @DisplayName("orphan-age below the presigned upload TTL is rejected")
    void orphanAgeMustExceedUploadTtl() {
        ConfigValidator v = validValidator();
        when(v.attachments.orphanAgeSeconds()).thenReturn(60L);   // below upload ttl 600
        IllegalStateException ex = assertThrows(IllegalStateException.class, v::validate);
        assertTrue(ex.getMessage().contains("orphan-age-seconds"), ex.getMessage());
    }

    @Test
    @DisplayName("frozen-days below history-days is rejected")
    void frozenRetentionMustBeAtLeastHistory() {
        ConfigValidator v = validValidator();
        when(v.retention.frozenDays()).thenReturn(30);   // below history 90
        IllegalStateException ex = assertThrows(IllegalStateException.class, v::validate);
        assertTrue(ex.getMessage().contains("frozen-days"), ex.getMessage());
    }

    @Test
    @DisplayName("all violations are accumulated and reported together")
    void accumulatesEveryViolation() {
        ConfigValidator v = validValidator();
        when(v.session.idleTimeoutMs()).thenReturn(0L);
        when(v.session.maxPending()).thenReturn(-1);
        when(v.deadLetter.cleanupBatch()).thenReturn(0);

        IllegalStateException ex = assertThrows(IllegalStateException.class, v::validate);
        assertTrue(ex.getMessage().contains("idle-timeout-ms"), ex.getMessage());
        assertTrue(ex.getMessage().contains("max-pending"), ex.getMessage());
        assertTrue(ex.getMessage().contains("deadletter.cleanup-batch"), ex.getMessage());
        assertTrue(ex.getMessage().contains("3 problem(s)"), ex.getMessage());
    }

    // ── valid mocked configs ─────────────────────────────────────────────────

    private MessengerConfig validMessenger() {
        MessengerConfig m = mock(MessengerConfig.class);

        MessengerConfig.Inbound inbound = mock(MessengerConfig.Inbound.class);
        when(inbound.queueSize()).thenReturn(3000);
        MessengerConfig.Inbound.PersistBatch pb = mock(MessengerConfig.Inbound.PersistBatch.class);
        when(pb.maxSize()).thenReturn(64);
        when(pb.lingerMs()).thenReturn(5);
        when(pb.maxConcurrentBatches()).thenReturn(8);
        when(inbound.persistBatch()).thenReturn(pb);
        when(m.inbound()).thenReturn(inbound);

        MessengerConfig.ReadBatch rb = mock(MessengerConfig.ReadBatch.class);
        when(rb.maxSize()).thenReturn(64);
        when(rb.lingerMs()).thenReturn(10);
        when(rb.maxConcurrentBatches()).thenReturn(4);
        when(m.readBatch()).thenReturn(rb);

        MessengerConfig.Outbound ob = mock(MessengerConfig.Outbound.class);
        when(ob.batchSize()).thenReturn(1500);
        when(ob.queueSize()).thenReturn(5000);
        when(m.outbound()).thenReturn(ob);

        MessengerConfig.StreamRetry sr = mock(MessengerConfig.StreamRetry.class);
        when(sr.minBackoffMs()).thenReturn(200);
        when(sr.maxBackoffMs()).thenReturn(5000);
        when(m.streamRetry()).thenReturn(sr);

        MessengerConfig.Feedback fb = mock(MessengerConfig.Feedback.class);
        when(fb.additionalMs()).thenReturn(1000);
        when(fb.adjustmentFactor()).thenReturn(2.0);
        when(fb.recoveryFactor()).thenReturn(0.5);
        when(fb.maxMs()).thenReturn(30000);
        when(m.feedback()).thenReturn(fb);

        MessengerConfig.Channel ch = mock(MessengerConfig.Channel.class);
        when(ch.minBackoffMs()).thenReturn(300);
        when(ch.maxBackoffMs()).thenReturn(1000);
        when(ch.maxRetries()).thenReturn(3);
        when(m.channel()).thenReturn(ch);

        MessengerConfig.Credits cr = mock(MessengerConfig.Credits.class);
        when(cr.maxValue()).thenReturn(100);
        when(cr.refillRatePerS()).thenReturn(10);
        when(m.credits()).thenReturn(cr);

        MessengerConfig.Idempotent id = mock(MessengerConfig.Idempotent.class);
        when(id.ttlSeconds()).thenReturn(30);
        when(m.idempotent()).thenReturn(id);

        MessengerConfig.ConversationCache cc = mock(MessengerConfig.ConversationCache.class);
        when(cc.maxSize()).thenReturn(100000L);
        when(cc.ttlSeconds()).thenReturn(60);
        when(m.conversationCache()).thenReturn(cc);

        return m;
    }

    private RateLimitConfig validRateLimit() {
        RateLimitConfig r = mock(RateLimitConfig.class);
        when(r.bucketCacheMax()).thenReturn(200000L);
        when(r.bucketIdleMinutes()).thenReturn(10);
        RateLimitConfig.Limit def = mock(RateLimitConfig.Limit.class);
        when(def.permitsPerSecond()).thenReturn(20.0);
        when(def.burst()).thenReturn(40);
        when(r.defaultLimit()).thenReturn(def);
        when(r.groups()).thenReturn(Map.of());
        return r;
    }

    private AttachmentsConfig validAttachments() {
        AttachmentsConfig a = mock(AttachmentsConfig.class);
        when(a.maxSizeBytes()).thenReturn(26214400L);
        when(a.orphanAgeSeconds()).thenReturn(3600L);
        when(a.cleanupBatch()).thenReturn(200);
        when(a.allowedContentTypes()).thenReturn(Optional.empty());
        return a;
    }

    private MinioConfig validMinio() {
        MinioConfig mi = mock(MinioConfig.class);
        when(mi.endpoint()).thenReturn("http://localhost:9000");
        when(mi.bucket()).thenReturn("messenger-attachments");
        when(mi.uploadTtlSeconds()).thenReturn(600);
        when(mi.downloadTtlSeconds()).thenReturn(300);
        return mi;
    }

    private SessionConfig validSession() {
        SessionConfig s = mock(SessionConfig.class);
        when(s.idleTimeoutMs()).thenReturn(35000L);
        when(s.maxPending()).thenReturn(1000);
        when(s.overloadKickBatch()).thenReturn(100);
        return s;
    }

    private RetentionConfig validRetention() {
        RetentionConfig rt = mock(RetentionConfig.class);
        when(rt.historyDays()).thenReturn(90);
        when(rt.frozenDays()).thenReturn(365);
        return rt;
    }

    private DeadLetterConfig validDeadLetter() {
        DeadLetterConfig dl = mock(DeadLetterConfig.class);
        when(dl.cleanupBatch()).thenReturn(500);
        return dl;
    }
}
