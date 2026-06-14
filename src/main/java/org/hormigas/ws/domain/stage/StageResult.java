package org.hormigas.ws.domain.stage;

import lombok.experimental.Accessors;

import java.util.Optional;

@Accessors(fluent = true)
public class StageResult<T> {

    private final StageStatus status;
    private final T updatedPayload;

    private StageResult(StageStatus stageStatus, T updatedPayload) {
        this.status = stageStatus;
        this.updatedPayload = updatedPayload;
    }

    enum StageStatus {UNKNOWN, UPDATED, PASSED, SKIPPED, FAILED}


    public static <T> StageResult<T> updated(T updatedPayload) {
        return new StageResult<>(StageStatus.UPDATED, updatedPayload);
    }

    public static <T> StageResult<T> passed() {
        return new StageResult<>(StageStatus.PASSED, null);
    }

    public static <T> StageResult<T> skipped() {
        return new StageResult<>(StageStatus.SKIPPED, null);
    }

    public static <T> StageResult<T> failed() {
        return new StageResult<>(StageStatus.FAILED, null);
    }

    public static <T> StageResult<T> unknown() {
        return new StageResult<>(StageStatus.UNKNOWN, null);
    }

    public boolean isSuccess() {
        return status == StageStatus.PASSED || status == StageStatus.UPDATED;
    }

    public boolean isSkipped() {
        return status == StageStatus.SKIPPED;
    }

    public boolean isFailed() {
        return status == StageStatus.FAILED;
    }

    public boolean isUpdated() {
        return status == StageStatus.UPDATED;
    }

    public boolean isPassed() {
        return status == StageStatus.PASSED;
    }

    public boolean isUnknown() {
        return status == StageStatus.UNKNOWN;
    }

    public T payload() {
        return updatedPayload;
    }
}


