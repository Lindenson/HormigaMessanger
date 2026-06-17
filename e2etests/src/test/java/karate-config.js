function fn() {
  var env = karate.env || 'dev';
  karate.log('karate.env =', env);

  var config = {
    // Messenger REST base + WS base (no /v1 prefix; endpoints under /api and /ws)
    baseUrl: 'http://localhost:8080',
    wsUrl:   'ws://localhost:8080/ws',

    // Test identities (master ↔ client pair + a third party for authz tests)
    masterId:    'test-master-1',
    masterName:  'Test Master 1',
    masterEmail: 'master1@test.com',
    clientId:    'test-client-1',
    clientName:  'Test Client 1',
    clientEmail: 'client1@test.com',
    otherId:     'test-other-9',
    otherName:   'Outsider',
    otherEmail:  'outsider@test.com'
  };

  if (env === 'staging') {
    config.baseUrl = 'http://91.99.6.25:8080';
    config.wsUrl   = 'ws://91.99.6.25:8080/ws';
  }

  karate.configure('connectTimeout', 2000);
  karate.configure('readTimeout', 5000);

  // Build the Ory Oathkeeper-style identity headers the service trusts.
  // role: 'MASTER' | 'CLIENT'
  config.idHeaders = function (id, name, role, email) {
    return {
      'X-User-Id': id,
      'X-User': name,
      'X-Role': role,
      'X-User-Email': email,
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
  };
  config.masterHeaders = function () { return config.idHeaders(config.masterId, config.masterName, 'MASTER', config.masterEmail); };
  config.clientHeaders = function () { return config.idHeaders(config.clientId, config.clientName, 'CLIENT', config.clientEmail); };
  config.noAuthHeaders = function () { return { 'Content-Type': 'application/json', 'Accept': 'application/json' }; };

  return config;
}
