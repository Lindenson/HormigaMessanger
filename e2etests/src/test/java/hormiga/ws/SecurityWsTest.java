package hormiga.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Security guarantee (Phase 0, defect S1): a message's attribution is taken from the authenticated
 * session and the conversation — never from the client-supplied {@code senderId}/{@code recipientId}.
 * A participant must not be able to post as the peer, nor deliver to a non-member by forging fields.
 */
@DisplayName("WS — security: authoritative sender/recipient attribution (anti-impersonation, S1)")
class SecurityWsTest {

    static final Duration T = Duration.ofSeconds(8);
    String uid, cId, mId, evil, conv;

    @BeforeEach
    void setup() {
        uid = UUID.randomUUID().toString();
        cId = "client-" + uid;
        mId = "master-" + uid;
        evil = "evil-" + uid;
        conv = Rest.createChat(cId, mId);
    }

    @Test
    @DisplayName("a forged senderId/recipientId is overwritten by the session + conversation")
    void spoofedAttributionIsRewritten() throws Exception {
        try (WsTestClient master = WsTestClient.connect(Rest.WS_URL, Rest.hdr(mId, "MASTER"));
             WsTestClient client = WsTestClient.connect(Rest.WS_URL, Rest.hdr(cId, "CLIENT"));
             WsTestClient intruder = WsTestClient.connect(Rest.WS_URL, Rest.hdr(evil, "CLIENT"))) {
            Thread.sleep(1200); // let sessions register

            // master is authenticated as mId, but forges senderId=cId (impersonate the peer)
            // and recipientId=evil (try to deliver to a non-member).
            String body = "spoof-" + uid;
            master.send(Msg.chatIn(/*forged sender*/ cId, /*forged recipient*/ evil, conv, uid, body, null));

            // the authentic recipient (the other participant) receives it, attributed to the real sender.
            JsonNode out = client.awaitType("CHAT_OUT", T);
            assertNotNull(out, "the authentic recipient (client) must receive the message");
            assertEquals(body, out.path("payload").path("body").asText());
            assertEquals(mId, out.path("senderId").asText(),
                    "senderId must be the authenticated session, NOT the forged value");
            assertEquals(cId, out.path("recipientId").asText(),
                    "recipientId must be derived from the conversation, NOT the forged value");

            // the forged recipient (not a participant) must receive nothing.
            assertNull(intruder.awaitType("CHAT_OUT", Duration.ofSeconds(2)),
                    "a non-participant must never receive the message via a forged recipientId");
        }
    }
}
