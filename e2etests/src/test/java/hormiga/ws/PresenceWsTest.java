package hormiga.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WS — presence (UC-U40): a connected client is tracked as online")
class PresenceWsTest {

    @Test
    @DisplayName("connecting a WS session marks the client present")
    void connectedClientIsPresent() throws Exception {
        String uid = UUID.randomUUID().toString();
        String cId = "client-" + uid;
        try (WsTestClient ignored = WsTestClient.connect(Rest.WS_URL, Rest.hdr(cId, "CLIENT"))) {
            Thread.sleep(1200); // let join/presence settle
            JsonNode presence = Rest.get("/api/presence", Rest.hdr(cId, "CLIENT")).body();
            assertNotNull(presence);
            assertTrue(presence.toString().contains(cId), "presence should list the connected client");
        }
    }
}
