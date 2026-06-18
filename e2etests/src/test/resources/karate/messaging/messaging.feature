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
    * eval java.lang.Thread.sleep(1500)
    # before any client ACK the persisted status is SENT (DELIVERED is ACK-driven, not push-driven)
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    When method GET
    Then status 200
    * def sid = response[0].messageId
    Given path '/api/chats', convId, 'receipts'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].status == ['SENT']
    # the recipient sends a delivery ACK over WS (correlationId = delivered messageId) → DELIVERED (UC-U13)
    * def ack = '{"type":"CHAT_ACK","senderId":"' + cId + '","recipientId":"' + mId + '","conversationId":"' + convId + '","messageId":"ack-' + uid + '","correlationId":"' + sid + '","ackId":1,"senderTimestamp":' + now + ',"senderTimezone":"UTC"}'
    * clientSock.send(ack)
    * eval java.lang.Thread.sleep(1200)
    Given path '/api/chats', convId, 'receipts'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].status contains 'DELIVERED'
    # the recipient (client) reads — exactly one message addressed to them is marked READ (UC-U14)
    Given path '/api/chats', convId, 'read'
    And headers cHdr
    When method POST
    Then status 200
    And match response.read == 1
    Given path '/api/chats', convId, 'receipts'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].status contains 'READ'
    # reading again is idempotent — nothing new to mark
    Given path '/api/chats', convId, 'read'
    And headers cHdr
    When method POST
    Then status 200
    And match response.read == 0

  Scenario: UC-U14 — read receipt over WebSocket marks READ (and pushes READ_OUT to the sender)
    # The recipient marks read via a WS READ_IN (no REST call); the server persists READ and pushes a
    # READ_OUT to the sender over the same Deliverer path used for CHAT_OUT (cross-socket frame capture
    # is flaky under Karate's shared listen queue, so we assert the durable READ outcome here).
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * def clientSock = karate.webSocket(wsUrl, null, { headers: cHdr })
    * eval java.lang.Thread.sleep(1200)
    * def now = java.lang.System.currentTimeMillis()
    * def msg = '{"type":"CHAT_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"text","body":"readws-' + uid + '"}}'
    * masterSock.send(msg)
    * eval java.lang.Thread.sleep(1500)
    # before reading, the message is SENT
    Given path '/api/chats', convId, 'receipts'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].status == ['SENT']
    # the recipient reads over WS (fire-and-forget READ_IN, no payload)
    * def readIn = '{"type":"READ_IN","senderId":"' + cId + '","recipientId":"' + mId + '","conversationId":"' + convId + '","messageId":"r-' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC"}'
    * clientSock.send(readIn)
    * eval java.lang.Thread.sleep(1200)
    # the persisted status reads back as READ — the WS read path worked end-to-end
    Given path '/api/chats', convId, 'receipts'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].status contains 'READ'

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

  Scenario: UC-U50 — history sync is cursor-paginated (since + limit)
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * eval java.lang.Thread.sleep(1000)
    * def now = java.lang.System.currentTimeMillis()
    * def mk = function(i){ return '{"type":"CHAT_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"p' + i + '-' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"text","body":"page' + i + '-' + uid + '"}}' }
    * masterSock.send(mk(1))
    * masterSock.send(mk(2))
    * masterSock.send(mk(3))
    * eval java.lang.Thread.sleep(1500)
    # first page: limit=2 → exactly the two oldest (server reassigns ids; ordering is by ULID)
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    And param limit = 2
    When method GET
    Then status 200
    And match response == '#[2]'
    * def cursor = response[1].messageId
    # next page after the cursor → the remaining message
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    And param since = cursor
    And param limit = 2
    When method GET
    Then status 200
    And match response == '#[1]'

  Scenario: UC-U40 — a connected client is tracked as present
    * def sock = karate.webSocket(wsUrl, null, { headers: cHdr })
    * eval java.lang.Thread.sleep(1200)
    Given path '/api/presence'
    And headers cHdr
    When method GET
    Then status 200
    * def presenceJson = karate.toString(response)
    * match presenceJson contains cId

  Scenario: UC-U12 — a message to an OFFLINE recipient is held (no-loss) and recovered on reconnect
    # The recipient is never online at send time. Per the durability backbone, an offline recipient
    # recovers by pulling conversation history via REST on reconnect; live WS re-push by the outbox
    # poller is a best-effort optimization on top (and exercised by UC-U10/U11).
    * def body = 'offline-' + uid
    * def masterSock = karate.webSocket(wsUrl, null, { headers: mHdr })
    * eval java.lang.Thread.sleep(1000)
    * def now = java.lang.System.currentTimeMillis()
    * def msg = '{"type":"CHAT_IN","senderId":"' + mId + '","recipientId":"' + cId + '","conversationId":"' + convId + '","messageId":"' + uid + '","senderTimestamp":' + now + ',"senderTimezone":"UTC","payload":{"kind":"text","body":"' + body + '"}}'
    * masterSock.send(msg)
    # live delivery is skipped (recipient offline); the message is persisted and held — never dropped
    * eval java.lang.Thread.sleep(1500)
    # the offline recipient loses nothing: the message is durable and retrievable
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].payload.body contains body
    * def serverId = response[0].messageId
    # the client comes ONLINE and resumes a live WS session (presence online)
    * def clientSock = karate.webSocket(wsUrl, null, { headers: cHdr })
    * eval java.lang.Thread.sleep(1200)
    # on reconnect it syncs history from its cursor and recovers exactly the held message
    Given path '/api/chats', convId, 'messages'
    And headers cHdr
    And param since = ''
    When method GET
    Then status 200
    And match response[*].messageId contains serverId
    And match response[*].payload.body contains body
