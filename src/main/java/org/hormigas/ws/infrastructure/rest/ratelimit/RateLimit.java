package org.hormigas.ws.infrastructure.rest.ratelimit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a REST resource (class) or endpoint (method) as belonging to a rate-limit <b>group</b> — the
 * {@link RateLimitFilter} uses the group's configured limit instead of the default. A method-level
 * annotation wins over a class-level one. Endpoints without it use the {@code default} group.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimit {
    /** The rate-limit group name (see {@code processing.rate-limit.groups.<name>}). */
    String value();
}
