package hormiga.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WS — must-arrive system notice, Strategy C (ADR-014)")
class SystemNoticeWsTest {

    static final Duration T = Duration.ofSeconds(8);

    @Test
    @DisplayName("admin notify → online recipient receives SYSTEM_OUT; SYSTEM_ACK is accepted (retracts the dead_letter draft)")
    void systemNoticeDeliveredAndAcked() throws Exception {
        String uid = UUID.randomUUID().toString();
        String cId = "client-" + uid;
        try (WsTestClient client = WsTestClient.connect(Rest.WS_URL, Rest.hdr(cId, "CLIENT"))) {
            Thread.sleep(1200);
            // server-originated notice via the ADMIN REST trigger (the C producer)
            Rest.Resp r = Rest.post("/api/system/notify",
                    "{\"recipientId\":\"" + cId + "\",\"kind\":\"event\",\"body\":\"overload-" + uid + "\"}",
                    Rest.hdr("admin-" + uid, "ADMIN"));
            assertEquals(202, r.status(), "notify should be accepted");

            JsonNode out = client.awaitType("SYSTEM_OUT", T);   // delivered by the poller via OUTBOUND_TRANSIENT
            assertNotNull(out, "client should receive SYSTEM_OUT");
            assertEquals("overload-" + uid, out.path("payload").path("body").asText());

            // confirm delivery — retracts the dead_letter DRAFT (the retract itself is unit/psql-verified)
            client.send(Msg.systemAck(cId, out.path("conversationId").asText(), "sysack-" + uid,
                    out.path("messageId").asText()));
            Thread.sleep(300);
        }
    }

    @Test
    @DisplayName("system notify requires ADMIN/SERVICE — a CLIENT caller is forbidden")
    void clientCannotNotify() {
        String uid = UUID.randomUUID().toString();
        Rest.Resp r = Rest.post("/api/system/notify",
                "{\"recipientId\":\"someone\",\"body\":\"x\"}", Rest.hdr("client-" + uid, "CLIENT"));
        assertEquals(403, r.status(), "a CLIENT must not be able to emit system notices");
    }
}
