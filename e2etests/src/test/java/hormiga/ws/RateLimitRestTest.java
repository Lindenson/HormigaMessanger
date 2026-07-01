package hormiga.ws;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("REST rate limiting — the strict attachments group returns 429 once its burst is exhausted")
class RateLimitRestTest {

    @Test
    @DisplayName("rapid attachment upload-url requests trip the per-caller limit (429)")
    void attachmentsBurstTrips429() {
        String uid = UUID.randomUUID().toString();
        String cId = "client-" + uid;
        String mId = "master-" + uid;
        String conv = Rest.createChat(cId, mId); // provisioned service-to-service

        String body = "{\"fileName\":\"f.png\",\"contentType\":\"image/png\",\"sizeBytes\":10}";
        boolean saw429 = false;
        int ok = 0;
        // attachments group: burst 5 @ 1/s → a rapid dozen must hit 429
        for (int i = 0; i < 12; i++) {
            int status = Rest.post("/api/chats/" + conv + "/attachments/upload-url", body, Rest.hdr(cId, "CLIENT"))
                    .status();
            if (status == 429) saw429 = true;
            else if (status == 201) ok++;
        }
        assertTrue(ok >= 1, "the first requests (within burst) succeed");
        assertTrue(saw429, "once the burst is spent, the limiter returns 429");
    }
}
