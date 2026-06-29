package org.hormigas.ws.domain.attachment;

import org.hormigas.ws.domain.message.Message;

/** Result of {@code confirmUpload}: on OK, {@code message} must be published into the delivery pipeline. */
public record ConfirmResult(UploadStatus status, Message message) {
    public static ConfirmResult of(UploadStatus s) { return new ConfirmResult(s, null); }
}
