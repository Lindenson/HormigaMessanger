package org.hormigas.ws.infrastructure.rest.ratelimit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.hormigas.ws.core.ratelimit.RateLimiter;
import org.hormigas.ws.infrastructure.security.IdentityHeaders;

import java.util.Map;

/**
 * Cross-cutting rate-limit gate for every {@code /api/*} REST endpoint (health/metrics under {@code /q}
 * are exempt). Post-matching, so it can read the matched resource/method's {@link RateLimit} group;
 * limits are per {@code (group, caller)} where the caller is the Ory {@code X-User-Id}. On exhaustion it
 * short-circuits with {@code 429}. This is transport policy — the bucket algorithm lives in core
 * ({@link RateLimiter}).
 */
@Provider
@ApplicationScoped
public class RateLimitFilter implements ContainerRequestFilter {

    static final String DEFAULT_GROUP = "default";
    static final String ANONYMOUS = "anonymous";

    @Inject
    RateLimiter rateLimiter;

    @Context
    ResourceInfo resourceInfo;

    @Override
    public void filter(ContainerRequestContext ctx) {
        String path = ctx.getUriInfo().getPath();
        if (path == null || !(path.startsWith("api/") || path.startsWith("/api/"))) {
            return; // only the public API is limited; /q health & metrics are exempt
        }
        String group = resolveGroup();
        String caller = callerId(ctx);
        if (!rateLimiter.tryAcquire(group, caller)) {
            ctx.abortWith(Response.status(429) // Too Many Requests
                    .entity(Map.of("error", "rate limit exceeded", "group", group))
                    .build());
        }
    }

    private String resolveGroup() {
        if (resourceInfo != null) {
            var method = resourceInfo.getResourceMethod();
            if (method != null && method.isAnnotationPresent(RateLimit.class)) {
                return method.getAnnotation(RateLimit.class).value();
            }
            var klass = resourceInfo.getResourceClass();
            if (klass != null && klass.isAnnotationPresent(RateLimit.class)) {
                return klass.getAnnotation(RateLimit.class).value();
            }
        }
        return DEFAULT_GROUP;
    }

    private String callerId(ContainerRequestContext ctx) {
        String id = ctx.getHeaderString(IdentityHeaders.USER_ID);
        return (id == null || id.isBlank()) ? ANONYMOUS : id;
    }
}
