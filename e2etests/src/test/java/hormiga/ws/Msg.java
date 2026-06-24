package hormiga.ws;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Builds the JSON wire frames the client sends over WS (server assigns its own ids on receipt). */
public final class Msg {

    private Msg() {}

    private static ObjectNode base(String type, String sender, String recipient, String conv, String mid) {
        ObjectNode n = WsTestClient.MAPPER.createObjectNode();
        n.put("type", type).put("senderId", sender).put("recipientId", recipient)
                .put("conversationId", conv).put("messageId", mid)
                .put("senderTimestamp", System.currentTimeMillis()).put("senderTimezone", "UTC");
        return n;
    }

    public static String chatIn(String sender, String recip, String conv, String mid, String body, String orderId) {
        ObjectNode n = base("CHAT_IN", sender, recip, conv, mid);
        ObjectNode p = n.putObject("payload");
        p.put("kind", "text").put("body", body);
        if (orderId != null) n.putObject("meta").put("orderId", orderId);
        return n.toString();
    }

    public static String chatAck(String sender, String recip, String conv, String mid, String correlationId, long ackId) {
        ObjectNode n = base("CHAT_ACK", sender, recip, conv, mid);
        n.put("correlationId", correlationId).put("ackId", ackId);
        return n.toString();
    }

    public static String readIn(String sender, String recip, String conv, String mid) {
        return base("READ_IN", sender, recip, conv, mid).toString();
    }

    public static String signalIn(String sender, String recip, String conv, String mid, String body) {
        ObjectNode n = base("SIGNAL_IN", sender, recip, conv, mid);
        n.putObject("payload").put("kind", "custom").put("body", body);
        return n.toString();
    }

    public static String systemAck(String sender, String conv, String mid, String correlationId) {
        ObjectNode n = base("SYSTEM_ACK", sender, "server", conv, mid);
        n.put("correlationId", correlationId);
        return n.toString();
    }
}
