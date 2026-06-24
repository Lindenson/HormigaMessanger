package hormiga.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WS — WebRTC signaling, Strategy S (UC-U30/H06)")
class SignalingWsTest {

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
    @DisplayName("SIGNAL_IN is delivered live to the online peer as SIGNAL_OUT")
    void deliveredToPeer() throws Exception {
        try (WsTestClient master = WsTestClient.connect(Rest.WS_URL, Rest.hdr(mId, "MASTER"));
             WsTestClient client = WsTestClient.connect(Rest.WS_URL, Rest.hdr(cId, "CLIENT"))) {
            Thread.sleep(1200);
            String sdp = "offer-" + uid;
            master.send(Msg.signalIn(mId, cId, conv, uid, sdp));

            JsonNode out = client.awaitType("SIGNAL_OUT", T);
            assertNotNull(out, "peer should receive SIGNAL_OUT");
            assertEquals(sdp, out.path("payload").path("body").asText());
        }
    }

    @Test
    @DisplayName("signaling is non-persistent — never written to History")
    void notPersisted() throws Exception {
        try (WsTestClient master = WsTestClient.connect(Rest.WS_URL, Rest.hdr(mId, "MASTER"));
             WsTestClient client = WsTestClient.connect(Rest.WS_URL, Rest.hdr(cId, "CLIENT"))) {
            Thread.sleep(1200);
            master.send(Msg.signalIn(mId, cId, conv, uid, "ephemeral-" + uid));
            Thread.sleep(1500);

            JsonNode history = Rest.get("/api/chats/" + conv + "/messages", Rest.hdr(cId, "CLIENT")).body();
            assertNotNull(history);
            assertTrue(history.isArray() && history.isEmpty(), "signaling must not appear in History");
        }
    }
}
