package org.hormigas.ws.domain.attachment;

/** Outcome of an attachment upload/confirm/download operation. */
public enum UploadStatus { OK, NOT_FOUND, FORBIDDEN, TOO_LARGE, NOT_UPLOADED, ALREADY_CONFIRMED }
