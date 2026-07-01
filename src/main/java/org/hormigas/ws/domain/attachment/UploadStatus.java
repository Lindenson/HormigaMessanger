package org.hormigas.ws.domain.attachment;

/** Outcome of an attachment upload/confirm/download operation.
 *  {@code OVERLOADED} = confirmed, but the pipeline ingress was full so the message wasn't emitted. */
public enum UploadStatus { OK, NOT_FOUND, FORBIDDEN, TOO_LARGE, NOT_UPLOADED, ALREADY_CONFIRMED, OVERLOADED }
