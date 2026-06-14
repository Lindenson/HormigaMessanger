package org.hormigas.ws.domain.validator.inout;

import jakarta.enterprise.context.ApplicationScoped;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.validator.ValidationResult;
import org.hormigas.ws.domain.validator.Validator;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates inbound {@link Message} instances. This validator enforces
 * structural correctness, semantic rules, and time consistency for all
 * incoming WebSocket messages.
 *
 * <h2>1. Allowed inbound message types</h2>
 * Only the following {@link MessageType} values are accepted:
 * <ul>
 *     <li>CHAT_IN</li>
 *     <li>SIGNAL_IN</li>
 *     <li>CHAT_ACK</li>
 * </ul>
 * All other message types (CHAT_OUT, SIGNAL_OUT, CHAT_ACK, PRESENT_*, SERVICE_OUT)
 * are invalid in inbound direction.
 *
 * <h2>2. Self-messaging is forbidden</h2>
 * senderId must not equal recipientId.
 *
 * <h2>3. ID validation</h2>
 * The following fields must be non-null, non-blank, and ≤ MAX_ID_LEN:
 * <ul>
 *     <li>senderId</li>
 *     <li>recipientId</li>
 *     <li>conversationId</li>
 *     <li>messageId</li>
 * </ul>
 *
 * For type {@link MessageType#CHAT_ACK}, correlationId is mandatory and must also
 * follow the same ID validation rules. (Reference resolution to previous outgoing
 * messages is performed on service layer, not here.)
 *
 * <h2>4. Timestamp validation</h2>
 * The client provides:
 * <ul>
 *     <li>senderTimestamp: epoch millis, required</li>
 *     <li>senderTimezone: IANA timezone string, required</li>
 * </ul>
 *
 * The validator interprets (senderTimestamp, senderTimezone) as a local timestamp
 * in the client’s timezone, converts it to UTC, and compares it with the server's
 * current time (Instant.now()).
 *
 * The absolute difference must not exceed MAX_CLOCK_SKEW_MS.
 *
 * <b>serverTimestamp from client is NOT validated</b> — the server overwrites it.
 *
 * <h2>5. Payload validation</h2>
 * payload must not be null.
 * payload.kind must not be null or blank.
 *
 * Kind-based rules:
 * <ul>
 *     <li>text → payload.body must contain non-blank text</li>
 *     <li>attachment → payload.body must contain non-blank reference</li>
 *     <li>event/custom → accepted without strict validation</li>
 *     <li>other → invalid kind</li>
 * </ul>
 *
 * <h2>6. Meta</h2>
 * meta may be null or empty.
 */
@ApplicationScoped
public class MessageValidator implements Validator<Message> {

    private static final int MAX_ID_LEN = 128;
    private static final long MAX_CLOCK_SKEW_MS = 5 * 60 * 1000;

    @Override
    public ValidationResult validate(Message msg) {
        List<String> errors = new ArrayList<>();

        validateInboundType(msg, errors);
        validateNoSelfMessaging(msg, errors);

        checkId(errors, msg.getSenderId(), "senderId");
        checkId(errors, msg.getRecipientId(), "recipientId");
        checkId(errors, msg.getConversationId(), "conversationId");
        checkId(errors, msg.getMessageId(), "messageId");

        // CHAT_ACK requires correlationId
        if (msg.getType() == MessageType.CHAT_ACK) {
            checkId(errors, msg.getCorrelationId(), "correlationId");
        }

        validateSenderTimestamp(msg, errors);
        validatePayload(msg, errors);

        return ValidationResult.of(errors);
    }

    /* ============================================================
     * Allowed inbound types
     * ============================================================ */
    private void validateInboundType(Message msg, List<String> errors) {
        if (msg.getType() == null) {
            errors.add("type: must not be null");
            return;
        }

        switch (msg.getType()) {
            case CHAT_IN, SIGNAL_IN, CHAT_ACK -> {
                // allowed
            }
            default -> errors.add("type: unsupported inbound message type: " + msg.getType());
        }
    }

    /* ============================================================ */

    private void validateNoSelfMessaging(Message msg, List<String> errors) {
        if (msg.getSenderId() != null &&
                msg.getRecipientId() != null &&
                msg.getSenderId().equals(msg.getRecipientId())) {

            errors.add("senderId and recipientId must not be equal (self-messaging is forbidden)");
        }
    }

    /* ============================================================ */

    private void checkId(List<String> errors, String value, String fieldName) {
        if (value == null || value.isBlank()) {
            errors.add(fieldName + ": must not be null or blank");
            return;
        }

        if (value.length() > MAX_ID_LEN) {
            errors.add(fieldName + ": length > " + MAX_ID_LEN);
        }
    }

    /* ============================================================ */

    private void validateSenderTimestamp(Message msg, List<String> errors) {
        long ts = msg.getSenderTimestamp();
        String tz = msg.getSenderTimezone();

        if (ts <= 0) {
            errors.add("senderTimestamp: must be > 0");
            return;
        }

        if (tz == null || tz.isBlank()) {
            errors.add("senderTimezone: must not be null or blank");
            return;
        }

        ZoneId zoneId;
        try {
            zoneId = ZoneId.of(tz);
        } catch (Exception ex) {
            errors.add("senderTimezone: invalid IANA timezone: " + tz);
            return;
        }

        ZonedDateTime clientZdt;
        try {
            clientZdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), zoneId);
        } catch (Exception ex) {
            errors.add("senderTimestamp: invalid epoch millis: " + ts);
            return;
        }

        Instant senderInstant = clientZdt.toInstant();
        Instant now = Instant.now();

        long diff = Math.abs(senderInstant.toEpochMilli() - now.toEpochMilli());
        if (diff > MAX_CLOCK_SKEW_MS) {
            errors.add("senderTimestamp: clock skew too large (" + diff + " ms)");
        }
    }

    /* ============================================================ */

    private void validatePayload(Message msg, List<String> errors) {
        Message.Payload p = msg.getPayload();

        if (p == null) {
            errors.add("payload: must not be null");
            return;
        }

        if (p.getKind() == null || p.getKind().isBlank()) {
            errors.add("payload.kind: must not be null or blank");
            return;
        }

        switch (p.getKind()) {
            case "text" -> {
                if (p.getBody() == null || p.getBody().isBlank()) {
                    errors.add("payload.body: must contain text for kind=text");
                }
            }
            case "attachment" -> {
                if (p.getBody() == null || p.getBody().isBlank()) {
                    errors.add("payload.body: must contain attachment reference");
                }
            }
            case "event", "custom" -> {
                // allowed
            }
            default -> errors.add("payload.kind: unknown kind=" + p.getKind());
        }
    }
}
