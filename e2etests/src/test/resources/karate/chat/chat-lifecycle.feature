@chat
Feature: Chat lifecycle (UC-U01, UC-U02, UC-H02)

  # M-2 first slice: universal idempotent create-chat (REST adapter), list, membership authz.
  # Unique pair per scenario so the run is independent of pre-existing data.

  Background:
    * url baseUrl
    * def uid = java.util.UUID.randomUUID() + ''
    * def c = 'client-' + uid
    * def m = 'master-' + uid

  Scenario: UC-U01 / UC-H02 — create a chat via REST is idempotent
    Given path '/api/chats'
    And headers serviceHeaders()
    And request { clientId: '#(c)', masterId: '#(m)', metadata: { orderId: 'order-123' } }
    When method POST
    Then status 201
    * def chatId = response.id
    And match response.clientId == c
    And match response.masterId == m
    # same pair again → same chat, no duplicate (idempotent), 200 not 201
    Given path '/api/chats'
    And headers serviceHeaders()
    And request { clientId: '#(c)', masterId: '#(m)', metadata: { orderId: 'order-456' } }
    When method POST
    Then status 200
    And match response.id == chatId

  Scenario: UC-U03 — a participant lists their chats
    # create a chat for this client, then list as that client
    Given path '/api/chats'
    And headers serviceHeaders()
    And request { clientId: '#(c)', masterId: '#(m)', metadata: {} }
    When method POST
    Then status 201
    Given path '/api/chats'
    And headers idHeaders(c, 'C', 'CLIENT', 'c@test.com')
    When method GET
    Then status 200
    And match response[*].id contains '#? _ != null'
    And match response[*].clientId contains c

  Scenario: UC-U61 — a non-participant cannot read a chat (403)
    Given path '/api/chats'
    And headers serviceHeaders()
    And request { clientId: '#(c)', masterId: '#(m)', metadata: {} }
    When method POST
    Then status 201
    * def chatId = response.id
    Given path '/api/chats', chatId, 'messages'
    And headers idHeaders(otherId, otherName, 'CLIENT', otherEmail)
    When method GET
    Then status 403

  Scenario: D4 — an end client may NOT create a chat (chats are provisioned service-to-service)
    Given path '/api/chats'
    And headers idHeaders(c, 'C', 'CLIENT', 'c@test.com')
    And request { clientId: '#(c)', masterId: '#(m)', metadata: {} }
    When method POST
    Then status 403
