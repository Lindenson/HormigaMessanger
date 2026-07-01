package org.hormigas.ws.infrastructure.rest.ratelimit;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.hormigas.ws.core.ratelimit.RateLimiter;
import org.hormigas.ws.infrastructure.rest.attachment.AttachmentResource;
import org.hormigas.ws.infrastructure.rest.conversation.ChatResource;
import org.hormigas.ws.infrastructure.security.IdentityHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("RateLimitFilter — limits /api/*, resolves the group, aborts 429 when denied")
class RateLimitFilterTest {

    private RateLimiter limiter;
    private ResourceInfo resourceInfo;
    private RateLimitFilter filter;

    @BeforeEach
    void setUp() {
        limiter = mock(RateLimiter.class);
        resourceInfo = mock(ResourceInfo.class);
        filter = new RateLimitFilter();
        filter.rateLimiter = limiter;
        filter.resourceInfo = resourceInfo;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ContainerRequestContext request(String path, String userId) {
        ContainerRequestContext ctx = mock(ContainerRequestContext.class);
        UriInfo uri = mock(UriInfo.class);
        when(uri.getPath()).thenReturn(path);
        when(ctx.getUriInfo()).thenReturn(uri);
        when(ctx.getHeaderString(IdentityHeaders.USER_ID)).thenReturn(userId);
        return ctx;
    }

    @Test
    @DisplayName("health/metrics under /q are exempt — the limiter is never consulted")
    void exemptsNonApi() {
        filter.filter(request("q/health/ready", null));
        verifyNoInteractions(limiter);
    }

    @Test
    @DisplayName("an /api request within the limit passes (no abort)")
    void allowsWithinLimit() {
        when(resourceInfo.getResourceClass()).thenReturn((Class) ChatResource.class); // no @RateLimit → default
        when(limiter.tryAcquire("default", "u1")).thenReturn(true);
        ContainerRequestContext ctx = request("api/chats", "u1");

        filter.filter(ctx);

        verify(limiter).tryAcquire("default", "u1");
        verify(ctx, never()).abortWith(any());
    }

    @Test
    @DisplayName("a class-level @RateLimit picks the group; over-limit → 429")
    void abortsOnDenyWithGroup() {
        when(resourceInfo.getResourceClass()).thenReturn((Class) AttachmentResource.class); // @RateLimit("attachments")
        when(limiter.tryAcquire("attachments", "u1")).thenReturn(false);
        ContainerRequestContext ctx = request("api/chats/c1/attachments/upload-url", "u1");

        filter.filter(ctx);

        ArgumentCaptor<Response> resp = ArgumentCaptor.forClass(Response.class);
        verify(ctx).abortWith(resp.capture());
        assertEquals429(resp.getValue());
    }

    @Test
    @DisplayName("missing identity → the caller key is 'anonymous'")
    void anonymousWhenNoIdentity() {
        when(resourceInfo.getResourceClass()).thenReturn((Class) ChatResource.class);
        when(limiter.tryAcquire("default", "anonymous")).thenReturn(true);
        filter.filter(request("api/chats", null));
        verify(limiter).tryAcquire("default", "anonymous");
    }

    private static void assertEquals429(Response r) {
        org.junit.jupiter.api.Assertions.assertEquals(429, r.getStatus());
    }
}
