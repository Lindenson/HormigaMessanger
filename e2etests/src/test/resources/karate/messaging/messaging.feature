@messaging @wip
Feature: Persistent messaging & delivery (UC-U10..U16) — SPEC, not yet implemented

  # WebSocket-driven. Karate WS client: listen, then assert delivery/ACK/read.
  # Drives M-2 (domain remodel) + M-3 (per-conversation ordering, single delivery authority).

  Background:
    * url baseUrl
    # WS connect carrying identity (Ory headers on the upgrade request)
    * configure headers = masterHeaders()

  Scenario: UC-U10/U11/U13 — send a persistent message, sender ACK, recipient delivery
    # master connects
    * def masterSocket = karate.webSocket(wsUrl, null, { headers: masterHeaders() })
    # client (recipient) connects and listens
    * def clientSocket = karate.webSocket(wsUrl, null, { headers: clientHeaders() })
    # master sends a CHAT message addressed to the client
    * def msg = { type: 'CHAT_IN', recipientId: '#(clientId)', payload: { kind: 'text', body: 'hello' }, messageId: 'm-1' }
    * masterSocket.send(msg)
    # sender receives an ACK (SENT)
    * def ack = masterSocket.listen(5000)
    * match ack contains { type: 'CHAT_ACK' }
    # recipient receives the message (delivered)
    * def delivered = clientSocket.listen(5000)
    * match delivered contains { payload: { body: 'hello' } }

  Scenario: UC-U12 — offline recipient gets the message on reconnect (history sync)
    # send while client offline → then client pulls conversation history since cursor
    # (encodes UC-U50 reconnect durability)
    * print 'spec placeholder — wire once chat + conversation-scoped history exist'
