package hormiga.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WS — offline recipient: held + recovered (UC-U12), poller live-redelivery on reconnect")
class OfflineRedeliveryWsTest {

    static final Duration T = Duration.ofSeconds(10);
    String uid, cId, mId, conv;

    @BeforeEach
    void setup() {
        uid = UUID.randomUUID().toString();
        cId = "client-" + uid;
        mId = "master-" + uid;
        conv = Rest.createChat(cId, mId);
    }

    @Test
    @DisplayName("message to an OFFLINE recipient is durable, and the poller re-pushes it live on reconnect")
    void heldThenRedeliveredOnReconnect() throws Exception {
        String body = "offline-" + uid;
        try (WsTestClient master = WsTestClient.connect(Rest.WS_URL, Rest.hdr(mId, "MASTER"))) {
            Thread.sleep(1000);
            master.send(Msg.chatIn(mId, cId, conv, uid, body, null)); // recipient is offline
            Thread.sleep(1500);

            // durable: retrievable via REST history while still offline (no loss)
            JsonNode history = Rest.get("/api/chats/" + conv + "/messages", Rest.hdr(cId, "CLIENT")).body();
            assertNotNull(history);
            assertTrue(history.toString().contains(body), "offline message must be durable in History");

            // recipient comes online → the outbox poller re-pushes it live (the redelivery fix)
            try (WsTestClient client = WsTestClient.connect(Rest.WS_URL, Rest.hdr(cId, "CLIENT"))) {
                JsonNode out = client.awaitType("CHAT_OUT", T);
                assertNotNull(out, "poller should re-push the held message on reconnect");
                assertEquals(body, out.path("payload").path("body").asText());
            }
        }
    }
}
