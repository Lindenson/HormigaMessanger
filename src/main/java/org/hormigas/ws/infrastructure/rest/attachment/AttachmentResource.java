package org.hormigas.ws.infrastructure.rest.attachment;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.hormigas.ws.core.attachment.Uploads;
import org.hormigas.ws.domain.attachment.ConfirmResult;
import org.hormigas.ws.infrastructure.rest.history.security.TokenVerifier;
import org.hormigas.ws.infrastructure.security.IdentityHeaders;

import java.util.Map;

/**
 * REST adapter for two-phase presigned-upload attachments (concept §10, ADR-010). All endpoints require
 * Ory identity headers; authorization and the on-confirm message emission live in the core use case
 * ({@code Uploads}) — this adapter only maps request → use case → HTTP. On confirm the core emits the
 * persistent message into the SAME inbound pipeline a WS chat message uses (via the emitter port).
 */
@Path("/api/chats/{chatId}/attachments")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AttachmentResource {

    @Inject
    Uploads attachments;

    @Inject
    TokenVerifier auth;

    public record UploadUrlRequest(String fileName, String contentType, Long sizeBytes) {}

    @POST
    @Path("/upload-url")
    public Uni<Response> uploadUrl(
            @PathParam("chatId") String chatId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email,
            UploadUrlRequest req) {
        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) return unauthorized();
        if (req == null || req.fileName() == null || req.fileName().isBlank()) {
            return badRequest("fileName is required");
        }
        if (req.sizeBytes() == null || req.sizeBytes() <= 0) {
            return badRequest("sizeBytes is required and must be > 0");
        }
        return attachments.requestUpload(chatId, caller.get().id(), req.fileName(), req.contentType(), req.sizeBytes())
                .map(r -> switch (r.status()) {
                    case OK -> Response.status(Response.Status.CREATED).entity(Map.of(
                            "attachmentId", r.ticket().attachmentId(),
                            "objectKey", r.ticket().objectKey(),
                            "uploadUrl", r.ticket().upload().url(),
                            "method", r.ticket().upload().method(),
                            "expiresAt", r.ticket().upload().expiresAt().toString())).build();
                    case NOT_FOUND -> status(Response.Status.NOT_FOUND);
                    case FORBIDDEN -> status(Response.Status.FORBIDDEN);
                    case TOO_LARGE -> status(Response.Status.REQUEST_ENTITY_TOO_LARGE);
                    case UNSUPPORTED_TYPE -> status(Response.Status.UNSUPPORTED_MEDIA_TYPE);
                    default -> status(Response.Status.BAD_REQUEST);
                });
    }

    @POST
    @Path("/{attachmentId}/confirm")
    public Uni<Response> confirm(
            @PathParam("chatId") String chatId,
            @PathParam("attachmentId") String attachmentId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {
        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) return unauthorized();
        return attachments.confirmUpload(attachmentId, caller.get().id())
                .map(r -> toConfirmResponse(r));
    }

    @GET
    @Path("/{attachmentId}/download-url")
    public Uni<Response> downloadUrl(
            @PathParam("chatId") String chatId,
            @PathParam("attachmentId") String attachmentId,
            @HeaderParam(IdentityHeaders.USER_ID) String userId,
            @HeaderParam(IdentityHeaders.USER_NAME) String userName,
            @HeaderParam(IdentityHeaders.USER_ROLE) String role,
            @HeaderParam(IdentityHeaders.USER_EMAIL) String email) {
        var caller = auth.fromHeaders(userId, userName, role, email);
        if (caller.isEmpty()) return unauthorized();
        return attachments.resolveDownload(attachmentId, caller.get().id())
                .map(r -> switch (r.status()) {
                    case OK -> Response.ok(Map.of(
                            "downloadUrl", r.download().url(),
                            "method", r.download().method(),
                            "expiresAt", r.download().expiresAt().toString())).build();
                    case FORBIDDEN -> status(Response.Status.FORBIDDEN);
                    default -> status(Response.Status.NOT_FOUND);
                });
    }

    private Response toConfirmResponse(ConfirmResult r) {
        return switch (r.status()) {
            // Core already emitted the message into the pipeline.
            case OK -> Response.status(Response.Status.ACCEPTED).entity(Map.of("status", "published")).build();
            // Confirmed, but the ingress was full — client should retry the confirm.
            case OVERLOADED -> Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(Map.of("status", "overloaded")).build();
            case ALREADY_CONFIRMED -> Response.ok(Map.of("status", "already-confirmed")).build();
            case NOT_UPLOADED -> Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("status", "not-uploaded")).build();
            case FORBIDDEN -> status(Response.Status.FORBIDDEN);
            default -> status(Response.Status.NOT_FOUND);
        };
    }

    private static Response status(Response.Status s) {
        return Response.status(s).build();
    }

    private static Uni<Response> badRequest(String msg) {
        return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST).entity(Map.of("error", msg)).build());
    }

    private static Uni<Response> unauthorized() {
        return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
    }
}
