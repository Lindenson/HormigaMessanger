package org.hormigas.ws.domain.conversation;

/** Whether a sender may send into a conversation right now. */
public enum SendCheck { ALLOW, NO_CONVERSATION, NOT_MEMBER, BLOCKED }
