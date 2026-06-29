package org.hormigas.ws.domain.conversation;

/**
 * Whether a sender may send into a conversation right now. Delete is "delete for me" and does NOT
 * close the chat for messaging — only {@code BLOCKED} (mutual) is terminal.
 */
public enum SendCheck { ALLOW, NO_CONVERSATION, NOT_MEMBER, BLOCKED }
