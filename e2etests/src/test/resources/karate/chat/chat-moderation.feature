@chat
Feature: Soft-delete & blacklist (UC-U03 soft-delete, UC-H07 block)

  Background:
    * url baseUrl
    * def uid = java.util.UUID.randomUUID() + ''
    * def c = 'client-' + uid
    * def m = 'master-' + uid
    * def cHdr = idHeaders(c, 'C', 'CLIENT', 'c@test.com')
    # create a chat for the pair (service-to-service / admin only — D4)
    Given path '/api/chats'
    And headers serviceHeaders()
    And request { clientId: '#(c)', masterId: '#(m)', metadata: {} }
    When method POST
    Then status 201
    * def chatId = response.id

  Scenario: UC-U03 — soft-delete hides the chat for the caller only
    # visible before
    Given path '/api/chats'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].id contains chatId
    # client soft-deletes
    Given path '/api/chats', chatId
    And headers cHdr
    When method DELETE
    Then status 204
    # now hidden for the client
    Given path '/api/chats'
    And headers cHdr
    When method GET
    Then status 200
    And match response[*].id !contains chatId
    # still present for the master (system retained it)
    Given path '/api/chats'
    And headers idHeaders(m, 'M', 'MASTER', 'm@test.com')
    When method GET
    Then status 200
    And match response[*].id contains chatId

  Scenario: UC-H07 — a participant can block and unblock the peer
    Given path '/api/chats', chatId, 'block'
    And headers cHdr
    When method POST
    Then status 204
    Given path '/api/chats', chatId, 'block'
    And headers cHdr
    When method DELETE
    Then status 204

  Scenario: UC-U61 — a non-participant cannot soft-delete (403)
    Given path '/api/chats', chatId
    And headers idHeaders(otherId, otherName, 'CLIENT', otherEmail)
    When method DELETE
    Then status 403
