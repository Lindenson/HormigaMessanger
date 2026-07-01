package hormiga.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WS — typing indicator, transient (Strategy S): routed, delivered live, never persisted")
class TypingWsTest {

    static final Duration T = Duration.ofSeconds(8);
    String uid, cId, mId, conv;

    @BeforeEach
    void setup() {
        uid = UUID.randomUUID().toString();
        cId = "client-" + uid;
        mId = "master-" + uid;
        conv = Rest.createChat(cId, mId);
    }

    @Test
    @DisplayName("TYPING_IN is delivered live to the online peer as TYPING_OUT")
    void deliveredToPeer() throws Exception {
        try (WsTestClient master = WsTestClient.connect(Rest.WS_URL, Rest.hdr(mId, "MASTER"));
             WsTestClient client = WsTestClient.connect(Rest.WS_URL, Rest.hdr(cId, "CLIENT"))) {
            Thread.sleep(1200);
            master.send(Msg.typingIn(mId, cId, conv, uid));

            JsonNode out = client.awaitType("TYPING_OUT", T);
            assertNotNull(out, "peer should receive TYPING_OUT");
            assertEquals(conv, out.path("conversationId").asText());
        }
    }

    @Test
    @DisplayName("typing is transient — never written to History")
    void notPersisted() throws Exception {
        try (WsTestClient master = WsTestClient.connect(Rest.WS_URL, Rest.hdr(mId, "MASTER"));
             WsTestClient client = WsTestClient.connect(Rest.WS_URL, Rest.hdr(cId, "CLIENT"))) {
            Thread.sleep(1200);
            master.send(Msg.typingIn(mId, cId, conv, uid));
            Thread.sleep(1200);

            JsonNode history = Rest.get("/api/chats/" + conv + "/messages", Rest.hdr(cId, "CLIENT")).body();
            assertNotNull(history);
            assertTrue(history.isArray() && history.isEmpty(), "typing must not appear in History");
        }
    }
}
