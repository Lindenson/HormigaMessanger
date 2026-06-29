@chat
Feature: Admin chat console (scope B — list/filter/stats, ADMIN only)

  Background:
    * url baseUrl
    * def uid = java.util.UUID.randomUUID() + ''
    * def c = 'client-' + uid
    * def m1 = 'master-a-' + uid
    * def m2 = 'master-b-' + uid
    * def adminHdr = idHeaders('admin-' + uid, 'Admin', 'ADMIN', 'admin@test.com')
    # provision two chats for the same client (service-to-service)
    Given path '/api/chats'
    And headers serviceHeaders()
    And request { clientId: '#(c)', masterId: '#(m1)', metadata: {} }
    When method POST
    Then status 201
    * def chat1 = response.id
    Given path '/api/chats'
    And headers serviceHeaders()
    And request { clientId: '#(c)', masterId: '#(m2)', metadata: {} }
    When method POST
    Then status 201
    * def chat2 = response.id

  Scenario: an admin lists chats filtered by participant (sees both, unfiltered by participant state)
    Given path '/api/admin/chats'
    And param participant = c
    And headers adminHdr
    When method GET
    Then status 200
    And match response.total == '#? _ >= 2'
    And match response.items[*].id contains chat1
    And match response.items[*].id contains chat2

  Scenario: an admin reads platform stats
    Given path '/api/admin/chats/stats'
    And headers adminHdr
    When method GET
    Then status 200
    And match response.total == '#? _ >= 2'
    And match response.blocked == '#? _ >= 0'

  Scenario: a non-admin (client) is forbidden from the admin console
    Given path '/api/admin/chats'
    And headers idHeaders(c, 'C', 'CLIENT', 'c@test.com')
    When method GET
    Then status 403
