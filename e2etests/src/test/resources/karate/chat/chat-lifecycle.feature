@chat @wip
Feature: Chat lifecycle (UC-U01, UC-U02, UC-H02) — SPEC, not yet implemented

  # ATDD spec: these drive M-2 (domain remodel) + the universal create-chat use case.
  # Tagged @wip → excluded from the default green run; drop the tag as each goes green.

  Background:
    * url baseUrl

  Scenario: UC-U01 / UC-H02 — create a chat via REST is idempotent
    # Universal create: REST adapter over the core CreateChat use case.
    Given path '/api/chats'
    And headers masterHeaders()
    And request { clientId: '#(clientId)', masterId: '#(masterId)', metadata: { orderId: 'order-123' } }
    When method POST
    Then status 201
    * def chatId = response.id
    # same pair again → same chat, no duplicate (idempotent)
    Given path '/api/chats'
    And headers masterHeaders()
    And request { clientId: '#(clientId)', masterId: '#(masterId)', metadata: { orderId: 'order-456' } }
    When method POST
    Then status 200
    And match response.id == chatId

  Scenario: UC-U03 — a participant lists their chats
    Given path '/api/chats'
    And headers clientHeaders()
    When method GET
    Then status 200
    And match response == '#array'

  Scenario: UC-U61 — a non-participant cannot read a chat (403)
    Given path '/api/chats'
    And headers masterHeaders()
    And request { clientId: '#(clientId)', masterId: '#(masterId)', metadata: {} }
    When method POST
    Then status 201
    * def chatId = response.id
    Given path '/api/chats', chatId, 'messages'
    And headers idHeaders(otherId, otherName, 'CLIENT', otherEmail)
    When method GET
    Then status 403
