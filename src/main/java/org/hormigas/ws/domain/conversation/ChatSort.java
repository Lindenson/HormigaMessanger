package org.hormigas.ws.domain.conversation;

/** Sort order for an admin chat listing. The SQL mapping lives in the infrastructure adapter. */
public enum ChatSort {
    CREATED_ASC,
    CREATED_DESC,
    UPDATED_ASC,
    /** Default: most-recently-active first. */
    UPDATED_DESC
}
