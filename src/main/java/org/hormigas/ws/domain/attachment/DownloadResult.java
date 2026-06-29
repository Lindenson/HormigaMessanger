package org.hormigas.ws.domain.attachment;

import org.hormigas.ws.domain.storage.PresignedUrl;

/** Result of {@code resolveDownload}: a status and (on OK) the presigned GET URL. */
public record DownloadResult(UploadStatus status, PresignedUrl download) {
    public static DownloadResult fail(UploadStatus s) { return new DownloadResult(s, null); }
}
