package org.hormigas.ws.infrastructure.storage.minio;

import io.minio.BucketExistsArgs;
import io.minio.GetPresignedObjectUrlArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.http.Method;
import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.storage.PresignedUrl;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MinioConfig;
import org.hormigas.ws.ports.storage.ObjectStorage;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MinIO adapter for {@link ObjectStorage} (ADR-010). The MinIO SDK is blocking, so every call is
 * offloaded to a worker thread — the reactive event loop is never blocked. The bucket is ensured
 * lazily on first use (not at startup), so the service boots even when MinIO is down (mirrors the
 * Kafka boot-independence decision).
 */
@Slf4j
@ApplicationScoped
public class MinioObjectStorage implements ObjectStorage {

    @Inject
    MinioClient client;

    @Inject
    MinioConfig config;

    String bucket;

    private final AtomicBoolean bucketReady = new AtomicBoolean(false);

    @PostConstruct
    void init() {
        bucket = config.bucket();
    }

    @Override
    public Uni<PresignedUrl> presignPut(String objectKey, String contentType, int ttlSeconds) {
        return ensureBucket().replaceWith(blocking(() -> {
            String url = client.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                    .method(Method.PUT)
                    .bucket(bucket)
                    .object(objectKey)
                    .expiry(ttlSeconds, TimeUnit.SECONDS)
                    .build());
            return new PresignedUrl(url, "PUT", expiry(ttlSeconds));
        }));
    }

    @Override
    public Uni<PresignedUrl> presignGet(String objectKey, int ttlSeconds) {
        return blocking(() -> {
            String url = client.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                    .method(Method.GET)
                    .bucket(bucket)
                    .object(objectKey)
                    .expiry(ttlSeconds, TimeUnit.SECONDS)
                    .build());
            return new PresignedUrl(url, "GET", expiry(ttlSeconds));
        });
    }

    @Override
    public Uni<Boolean> exists(String objectKey) {
        return blocking(() -> {
            try {
                client.statObject(StatObjectArgs.builder().bucket(bucket).object(objectKey).build());
                return true;
            } catch (ErrorResponseException e) {
                String code = e.errorResponse() == null ? "" : e.errorResponse().code();
                if ("NoSuchKey".equals(code) || "NoSuchObject".equals(code)) {
                    return false;
                }
                throw new RuntimeException("statObject failed for " + objectKey, e);
            } catch (Exception e) {
                throw new RuntimeException("statObject failed for " + objectKey, e);
            }
        });
    }

    @Override
    public Uni<Long> size(String objectKey) {
        return blocking(() -> {
            try {
                return client.statObject(StatObjectArgs.builder().bucket(bucket).object(objectKey).build()).size();
            } catch (ErrorResponseException e) {
                String code = e.errorResponse() == null ? "" : e.errorResponse().code();
                if ("NoSuchKey".equals(code) || "NoSuchObject".equals(code)) {
                    return -1L;
                }
                throw new RuntimeException("statObject(size) failed for " + objectKey, e);
            } catch (Exception e) {
                throw new RuntimeException("statObject(size) failed for " + objectKey, e);
            }
        });
    }

    @Override
    public Uni<Void> delete(String objectKey) {
        return Uni.createFrom().<Void>item(() -> {
                    try {
                        client.removeObject(RemoveObjectArgs.builder().bucket(bucket).object(objectKey).build());
                    } catch (Exception e) {
                        log.warn("Best-effort delete of {} failed (ignored): {}", objectKey, e.getMessage());
                    }
                    return null;
                })
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /** Idempotent, cached bucket creation; runs once per JVM after the first success. */
    private Uni<Void> ensureBucket() {
        if (bucketReady.get()) {
            return Uni.createFrom().voidItem();
        }
        return Uni.createFrom().<Void>item(() -> {
                    try {
                        boolean found = client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
                        if (!found) {
                            client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
                            log.info("Created MinIO bucket {}", bucket);
                        }
                        bucketReady.set(true);
                    } catch (Exception e) {
                        throw new RuntimeException("Could not ensure MinIO bucket " + bucket, e);
                    }
                    return null;
                })
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private static <T> Uni<T> blocking(java.util.concurrent.Callable<T> work) {
        return Uni.createFrom().item(() -> {
                    try {
                        return work.call();
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private static Instant expiry(int ttlSeconds) {
        return Instant.now().plusSeconds(ttlSeconds);
    }
}
