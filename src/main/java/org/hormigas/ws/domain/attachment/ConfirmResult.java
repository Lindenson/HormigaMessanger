package org.hormigas.ws.domain.attachment;

/**
 * Result of {@code confirmUpload}: just the outcome. On {@code OK} the core has already emitted the
 * attachment message into the delivery pipeline (via the {@code ChatMessageEmitter} port); on
 * {@code OVERLOADED} it was confirmed but the ingress was full. The REST adapter only maps status → HTTP.
 */
public record ConfirmResult(UploadStatus status) {
    public static ConfirmResult of(UploadStatus s) {
        return new ConfirmResult(s);
    }
}
