package org.hormigas.ws.domain.conversation;

/** Result of an idempotent create: the chat and whether it was newly created (201 vs 200). */
public record CreateResult(Conversation conversation, boolean created) {}
