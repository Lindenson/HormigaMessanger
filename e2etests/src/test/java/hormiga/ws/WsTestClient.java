package hormiga.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Minimal WebSocket test client over the JDK's built-in {@link java.net.http.WebSocket} — a real WS
 * client (no Karate, no GraalVM JS handler, no license). Connects to the running app with Ory
 * identity headers, sends frames, and captures incoming text frames into a queue for deterministic,
 * per-socket assertions (unlike Karate's shared listen-queue).
 */
public final class WsTestClient implements AutoCloseable {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    private final WebSocket ws;
    private final BlockingQueue<String> frames;

    private WsTestClient(WebSocket ws, BlockingQueue<String> frames) {
        this.ws = ws;
        this.frames = frames;
    }

    public static WsTestClient connect(String url, Map<String, String> headers) {
        BlockingQueue<String> q = new LinkedBlockingQueue<>();
        WebSocket.Builder b = HttpClient.newHttpClient().newWebSocketBuilder();
        headers.forEach(b::header);
        WebSocket ws = b.buildAsync(URI.create(url), new FrameListener(q)).join();
        return new WsTestClient(ws, q);
    }

    public void send(String json) {
        ws.sendText(json, true).join();
    }

    /** Await the first frame whose parsed JSON matches {@code predicate}; returns null on timeout. */
    public JsonNode await(Predicate<JsonNode> predicate, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            while (System.nanoTime() < deadline) {
                long remMs = Math.max(0, (deadline - System.nanoTime()) / 1_000_000);
                String frame = frames.poll(remMs, TimeUnit.MILLISECONDS);
                if (frame == null) break;
                JsonNode node = MAPPER.readTree(frame);
                if (predicate.test(node)) return node;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /** Await the first frame of the given {@code type} (intervening frames are discarded). */
    public JsonNode awaitType(String type, Duration timeout) {
        return await(n -> type.equals(n.path("type").asText(null)), timeout);
    }

    /** Drain all frames currently captured, parsed. */
    public List<JsonNode> drain() {
        List<String> raw = new ArrayList<>();
        frames.drainTo(raw);
        List<JsonNode> out = new ArrayList<>();
        for (String s : raw) {
            try { out.add(MAPPER.readTree(s)); } catch (Exception ignored) { }
        }
        return out;
    }

    @Override
    public void close() {
        try { ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join(); } catch (Exception ignored) { }
    }

    /** Accumulates (possibly fragmented) text frames and enqueues each complete message. */
    private static final class FrameListener implements WebSocket.Listener {
        private final BlockingQueue<String> q;
        private final StringBuilder buf = new StringBuilder();

        FrameListener(BlockingQueue<String> q) { this.q = q; }

        @Override public void onOpen(WebSocket ws) { ws.request(1); }

        @Override public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            buf.append(data);
            if (last) { q.add(buf.toString()); buf.setLength(0); }
            ws.request(1);
            return null;
        }

        @Override public void onError(WebSocket ws, Throwable error) {
            // tests assert on absence of expected frames, not on transport errors
        }
    }
}
