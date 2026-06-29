package org.hormigas.ws.domain.attachment;

/** Result of {@code requestUpload}: a status and (on OK) the upload ticket. */
public record UploadResult(UploadStatus status, UploadTicket ticket) {
    public static UploadResult fail(UploadStatus s) { return new UploadResult(s, null); }
}
