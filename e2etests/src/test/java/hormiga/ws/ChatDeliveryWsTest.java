package hormiga.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WS — persistent chat delivery (UC-U10/U11) + sender SENT-ack (UC-M10)")
class ChatDeliveryWsTest {

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
    @DisplayName("CHAT_IN from master → online client receives the delivered CHAT_OUT")
    void deliveredToOnlineRecipient() throws Exception {
        try (WsTestClient master = WsTestClient.connect(Rest.WS_URL, Rest.hdr(mId, "MASTER"));
             WsTestClient client = WsTestClient.connect(Rest.WS_URL, Rest.hdr(cId, "CLIENT"))) {
            Thread.sleep(1200); // let both sessions register
            String body = "hi-" + uid;
            master.send(Msg.chatIn(mId, cId, conv, uid, body, null));

            JsonNode out = client.awaitType("CHAT_OUT", T);
            assertNotNull(out, "client should receive a CHAT_OUT");
            assertEquals(body, out.path("payload").path("body").asText());
            assertEquals(cId, out.path("recipientId").asText());
        }
    }

    @Test
    @DisplayName("the server ACKs the sender (SENT) — CHAT_ACK with correlationId = the sent messageId")
    void senderReceivesSentAck() throws Exception {
        try (WsTestClient master = WsTestClient.connect(Rest.WS_URL, Rest.hdr(mId, "MASTER"))) {
            Thread.sleep(1000);
            master.send(Msg.chatIn(mId, cId, conv, uid, "ack-" + uid, null));

            JsonNode ack = master.awaitType("CHAT_ACK", T);
            assertNotNull(ack, "sender should receive a CHAT_ACK");
            assertEquals(uid, ack.path("correlationId").asText());
        }
    }
}
