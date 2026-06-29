package org.hormigas.ws.domain.conversation;

/** A send check plus the resolved conversation (non-null only when {@code check == ALLOW}). */
public record SendDecision(SendCheck check, Conversation conversation) {}
