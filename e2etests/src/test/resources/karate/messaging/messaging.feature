@messaging
Feature: Persistent messaging & delivery (UC-U10/U11/U13)

  # WebSocket end-to-end: create a chat (so the send-guard allows), connect both participants,
  # master sends CHAT_IN, the online client receives the delivered CHAT_OUT.

  Background:
    * url baseUrl
    * def uid = java.util.UUID.randomUUID() + ''
    * def cId = 'client-' + uid
    * def mId = 'master-' + uid
    * def mHdr = idHeaders(mId, 'M', 'MASTER', 'm@test.com')
    * def cHdr = idHeaders(cId, 'C', 'CLIENT', 'c@test.com')
    # create the conversation (REST) — membership the send-guard checks
    Given path '/api/chats'
    And headers mHdr
    And request { clientId: '#(cId)', masterId: '#(mId)', metadata: {} }
    When method POST
    Then status 201
    * def convId = response.id

  Scenario: UC-U10/U11 — persistent message delivered to the online recipient
    * def body = 'hello-' + uid
    # master connects first; the recipient (client) connects LAST so `listen` targets it.
    # null handler (Karate 1.4.1 + GraalVM cannot build a JsFunction handler on this JDK), so we
    # collect a few frames in arrival order and assert the delivered chat appears among them
    # (presence/join frames may precede it).
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * def clientSock = karate.webSocket(wsUrl, null, { headers: cHdr })
    # let both onOpen/registration complete before sending (else sender is unregistered or the
    # recipient has no live session yet and the message is held in the outbox, not delivered live)
    * eval java.lang.Thread.sleep(1200)
    * def now = java.lang.System.currentTimeMillis()
    * def msg = '{"type":"CHAT_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"text","body":"' + body + '"}}'
    * masterSock.send(msg)
    # collect up to 3 frames (a presence/join frame may precede the chat); assert the chat is among them
    * listen 5000
    * def f1 = (listenResult == null ? '{}' : ('' + listenResult))
    * listen 2000
    * def f2 = (listenResult == null ? '{}' : ('' + listenResult))
    * listen 2000
    * def f3 = (listenResult == null ? '{}' : ('' + listenResult))
    * json frames = '[' + f1 + ',' + f2 + ',' + f3 + ']'
    * match frames[*].type contains 'CHAT_OUT'
    * match frames[*].payload.body contains body

  Scenario: UC-M10 — sender receives a server ACK (SENT) for a persistent message
    # only the sender needs to be online; the server ACKs on persist (correlationId = sent messageId)
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * eval java.lang.Thread.sleep(1000)
    * def now = java.lang.System.currentTimeMillis()
    * def msg = '{"type":"CHAT_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"text","body":"ack-' + uid + '"}}'
    * masterSock.send(msg)
    * listen 5000
    * def a1 = (listenResult == null ? '{}' : ('' + listenResult))
    * listen 2000
    * def a2 = (listenResult == null ? '{}' : ('' + listenResult))
    * json acks = '[' + a1 + ',' + a2 + ']'
    * match acks[*].type contains 'CHAT_ACK'
    * match acks[*].correlationId contains uid

  Scenario: UC-U21 — a participant can delete a non-frozen message
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * def clientSock = karate.webSocket(wsUrl, null, { headers: cHdr })
    * eval java.lang.Thread.sleep(1200)
    * def now = java.lang.System.currentTimeMillis()
    * def msg = '{"type":"CHAT_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"text","body":"del-' + uid + '"}}'
    * masterSock.send(msg)
    * eval java.lang.Thread.sleep(1500)
    # find the persisted message's server id, delete it
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    When method GET
    Then status 200
    And match response == '#[_ > 0]'
    * def mid = response[0].messageId
    Given path '/api/chats', convId, 'messages', mid
    And headers cHdr
    When method DELETE
    Then status 204
    # gone from history
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].messageId !contains mid

  Scenario: UC-U22 — freeze is message-level scoped by orderId; frozen messages cannot be deleted (409)
    # two orders in the SAME (order-agnostic) chat — a contract on orderA must not freeze orderB
    * def orderA = 'order-A-' + uid
    * def orderB = 'order-B-' + uid
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * def clientSock = karate.webSocket(wsUrl, null, { headers: cHdr })
    * eval java.lang.Thread.sleep(1200)
    * def now = java.lang.System.currentTimeMillis()
    * def msgA = '{"type":"CHAT_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"a-' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"text","body":"frzA-' + uid + '"},"meta":{"orderId":"' + orderA + '"}}'
    * def msgB = '{"type":"CHAT_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"b-' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"text","body":"frzB-' + uid + '"},"meta":{"orderId":"' + orderB + '"}}'
    * masterSock.send(msgA)
    * masterSock.send(msgB)
    * eval java.lang.Thread.sleep(1500)
    # freeze requires an orderId — a freeze with no order is a bad request (no chat-wide freeze)
    Given path '/api/chats', convId, 'freeze'
    And headers cHdr
    And request {}
    When method POST
    Then status 400
    # freeze only orderA's messages (contract reached for that order) — exactly one frozen
    Given path '/api/chats', convId, 'freeze'
    And headers cHdr
    And request { orderId: '#(orderA)' }
    When method POST
    Then status 200
    And match response.frozen == 1
    # resolve the server-assigned messageIds (the server reassigns ids; we match by body)
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    When method GET
    Then status 200
    * def midOf = function(body){ return karate.jsonPath(response, "$[?(@.payload.body=='" + body + "')].messageId")[0] }
    * def aMid = midOf('frzA-' + uid)
    * def bMid = midOf('frzB-' + uid)
    # orderA message delete is now rejected (immutable history)
    Given path '/api/chats', convId, 'messages', aMid
    And headers cHdr
    When method DELETE
    Then status 409
    # orderB message in the SAME chat is untouched — still deletable
    Given path '/api/chats', convId, 'messages', bMid
    And headers cHdr
    When method DELETE
    Then status 204

  Scenario: UC-U13/U14 — status machine SENT→DELIVERED→READ; READ mark is idempotent
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * def clientSock = karate.webSocket(wsUrl, null, { headers: cHdr })
    * eval java.lang.Thread.sleep(1200)
    * def now = java.lang.System.currentTimeMillis()
    * def msg = '{"type":"CHAT_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"text","body":"read-' + uid + '"}}'
    * masterSock.send(msg)
    # client is online → the message is delivered, so status advances SENT→DELIVERED (UC-U13)
    * eval java.lang.Thread.sleep(1500)
    Given path '/api/chats', convId, 'receipts'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].status contains 'DELIVERED'
    # the recipient (client) acknowledges — exactly one message addressed to them is marked READ
    Given path '/api/chats', convId, 'read'
    And headers cHdr
    When method POST
    Then status 200
    And match response.read == 1
    # per-message status reads back as READ (UC-U14)
    Given path '/api/chats', convId, 'receipts'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].status contains 'READ'
    # acknowledging again is idempotent — nothing new to mark
    Given path '/api/chats', convId, 'read'
    And headers cHdr
    When method POST
    Then status 200
    And match response.read == 0
    # acknowledging again is idempotent — nothing new to mark
    Given path '/api/chats', convId, 'read'
    And headers cHdr
    When method POST
    Then status 200
    And match response.read == 0

  Scenario: UC-U50/U12 — a sent message is durable and retrievable via conversation history
    * def body = 'persist-' + uid
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * def clientSock = karate.webSocket(wsUrl, null, { headers: cHdr })
    * eval java.lang.Thread.sleep(1200)
    * def now = java.lang.System.currentTimeMillis()
    * def msg = '{"type":"CHAT_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"text","body":"' + body + '"}}'
    * masterSock.send(msg)
    # let the persist (History + Outbox) settle, then pull the conversation history via REST
    * eval java.lang.Thread.sleep(1500)
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].payload.body contains body
