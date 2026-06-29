package org.hormigas.ws.domain.conversation;

/**
 * Aggregate counts for the admin dashboard.
 *
 * @param total   all conversations on the platform
 * @param blocked conversations blocked by either side
 */
public record ChatStats(long total, long blocked) {}
