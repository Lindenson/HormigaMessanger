@signaling
Feature: WebRTC call signaling (Strategy S, UC-U30 / UC-H06)

  # Signaling messages are real-time, non-persistent (Strategy S): delivered live to the online peer,
  # idempotent, never written to History/Outbox, and dropped if the peer is offline. They carry WebRTC
  # offer/answer/ICE negotiation between the two participants of a chat. The membership guard applies
  # exactly as for chat (UC-U61) — only the delivery strategy differs (cached, not persistent).

  Background:
    * url baseUrl
    * def uid = java.util.UUID.randomUUID() + ''
    * def cId = 'client-' + uid
    * def mId = 'master-' + uid
    * def mHdr = idHeaders(mId, 'M', 'MASTER', 'm@test.com')
    * def cHdr = idHeaders(cId, 'C', 'CLIENT', 'c@test.com')
    # create the conversation (REST) — service-to-service / admin only (D4); send-guard checks membership
    Given path '/api/chats'
    And headers serviceHeaders()
    And request { clientId: '#(cId)', masterId: '#(mId)', metadata: {} }
    When method POST
    Then status 201
    * def convId = response.id

  @wip
  # Live cross-socket WS capture is unreliable under Karate 1.4.1 + GraalVM on this JDK (the message
  # handler returns null — same limitation noted for CHAT_OUT in messaging.feature). Live SIGNAL_OUT
  # delivery is instead verified deterministically by e2etests/wsdelivery-check.js (a raw `ws` client),
  # which observes CLIENT frames = [PRESENT_INIT, PRESENT_JOIN, CHAT_OUT, SIGNAL_OUT]. The two
  # non-@wip scenarios below assert the Strategy-S guarantees (non-persistence) deterministically.
  Scenario: UC-U30/H06 — a signaling offer is delivered live to the online peer
    # master connects first; the recipient (client) connects LAST so `listen` targets it.
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * def clientSock = karate.webSocket(wsUrl, null, { headers: cHdr })
    * eval java.lang.Thread.sleep(1200)
    * def now = java.lang.System.currentTimeMillis()
    * def sdp = 'offer-sdp-' + uid
    # WebRTC offer as a signaling message (kind=custom carries the negotiation payload)
    * def offer = '{"type":"SIGNAL_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"custom","body":"' + sdp + '"}}'
    * masterSock.send(offer)
    # collect a few frames (a presence/join frame may precede); assert the SIGNAL_OUT is among them
    * listen 5000
    * def f1 = (listenResult == null ? '{}' : ('' + listenResult))
    * listen 2000
    * def f2 = (listenResult == null ? '{}' : ('' + listenResult))
    * listen 2000
    * def f3 = (listenResult == null ? '{}' : ('' + listenResult))
    * json frames = '[' + f1 + ',' + f2 + ',' + f3 + ']'
    * match frames[*].type contains 'SIGNAL_OUT'
    * match frames[*].payload.body contains sdp

  Scenario: UC-U30 — signaling is non-persistent (never written to History)
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * def clientSock = karate.webSocket(wsUrl, null, { headers: cHdr })
    * eval java.lang.Thread.sleep(1200)
    * def now = java.lang.System.currentTimeMillis()
    * def offer = '{"type":"SIGNAL_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"custom","body":"ephemeral-' + uid + '"}}'
    * masterSock.send(offer)
    * eval java.lang.Thread.sleep(1500)
    # nothing persisted: the conversation history is empty (signaling bypasses History/Outbox)
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    When method GET
    Then status 200
    And match response == '#[0]'

  Scenario: UC-U30 — a signaling offer to an OFFLINE peer is dropped (not held, not persisted)
    # only the sender is online; the peer never connects → the signal cannot be delivered and, being
    # non-persistent, is simply dropped (no outbox hold, unlike a persistent chat message).
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * eval java.lang.Thread.sleep(1000)
    * def now = java.lang.System.currentTimeMillis()
    * def offer = '{"type":"SIGNAL_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"custom","body":"dropped-' + uid + '"}}'
    * masterSock.send(offer)
    * eval java.lang.Thread.sleep(1500)
    # the peer comes online afterwards and syncs history — there is nothing to recover (dropped)
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    When method GET
    Then status 200
    And match response == '#[0]'
