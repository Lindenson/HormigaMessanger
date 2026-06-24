package hormiga.ws;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

/** REST setup helper for the WS suite (create chats, trigger system notices, read history). */
public final class Rest {

    public static final String BASE = System.getProperty("e2e.baseUrl", "http://localhost:8080");
    public static final String WS_URL = System.getProperty("e2e.wsUrl", "ws://localhost:8080/ws");

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private Rest() {}

    /** Ory identity headers (the gateway trust model — the app trusts these verbatim). */
    public static Map<String, String> hdr(String id, String role) {
        return Map.of(
                "X-User-Id", id,
                "X-User", role,
                "X-Role", role.toUpperCase(),
                "X-User-Email", id + "@test.eu");
    }

    public record Resp(int status, JsonNode body) {}

    public static Resp post(String path, String json, Map<String, String> headers) {
        return send("POST", path, json, headers);
    }

    public static Resp get(String path, Map<String, String> headers) {
        return send("GET", path, null, headers);
    }

    private static Resp send(String method, String path, String json, Map<String, String> headers) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(BASE + path))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json");
            headers.forEach(b::header);
            b.method(method, json == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(json));
            HttpResponse<String> r = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
            JsonNode body = (r.body() == null || r.body().isBlank())
                    ? null : WsTestClient.MAPPER.readTree(r.body());
            return new Resp(r.statusCode(), body);
        } catch (Exception e) {
            throw new RuntimeException(method + " " + path + " failed", e);
        }
    }

    /** Create the (client, master) conversation and return its id. */
    public static String createChat(String clientId, String masterId) {
        Resp r = post("/api/chats",
                "{\"clientId\":\"" + clientId + "\",\"masterId\":\"" + masterId + "\",\"metadata\":{}}",
                hdr(masterId, "MASTER"));
        if (r.status() != 201 && r.status() != 200) {
            throw new IllegalStateException("createChat failed: HTTP " + r.status());
        }
        return r.body().get("id").asText();
    }
}
