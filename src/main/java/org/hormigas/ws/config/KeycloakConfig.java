package org.hormigas.ws.config;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "security.keycloak")
public interface KeycloakConfig {

    Cert cert();
    Request request();

    interface Cert {
        String url();
    }

    interface Request {
        int rateLimit();
    }
}
