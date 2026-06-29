package org.hormigas.ws.core.attachment;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.attachment.ConfirmResult;
import org.hormigas.ws.domain.attachment.DownloadResult;
import org.hormigas.ws.domain.attachment.UploadResult;

/**
 * Driving port (port-IN) for the two-phase presigned-upload attachment use case (concept §10,
 * ADR-010). The REST adapter depends on this interface; the core ({@code core.attachment.Attachments})
 * implements it. Result/value types are {@code domain} types (see {@code domain.attachment}); the port
 * merely references them.
 */
public interface Uploads {

    /** Phase 1: validate membership/size, persist a PENDING row, return a presigned PUT URL. */
    Uni<UploadResult> requestUpload(String conversationId, String uploaderId,
                                    String fileName, String contentType, Long sizeBytes);

    /** Phase 3: verify the object, mark CONFIRMED, build the persistent attachment chat message. */
    Uni<ConfirmResult> confirmUpload(String attachmentId, String callerId);

    /** Read time: membership check → presigned GET URL for a CONFIRMED attachment. */
    Uni<DownloadResult> resolveDownload(String attachmentId, String callerId);
}
