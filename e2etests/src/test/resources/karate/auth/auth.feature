@auth
Feature: Identity & authorization (UC-U60, UC-U61)

  Background:
    * url baseUrl

  Scenario: UC-U60 — REST without identity headers is rejected (401)
    Given path '/api/history'
    And headers noAuthHeaders()
    When method GET
    Then status 401

  Scenario: UC-U60 — REST with Ory identity headers is accepted (200)
    Given path '/api/history'
    And headers clientHeaders()
    When method GET
    Then status 200
    And match response == '#array'
