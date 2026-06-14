package org.hormigas.ws.infrastructure.websocket.security;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hormigas.ws.config.KeycloakConfig;
import org.hormigas.ws.domain.credentials.ClientData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Optional;

@ApplicationScoped
public class JwtValidator {

    private static final Logger log = LoggerFactory.getLogger(JwtValidator.class);

    @Inject
    KeycloakConfig keycloakConfig;

    ConfigurableJWTProcessor<SecurityContext> jwtProcessor;

    @PostConstruct
    void init() {
        try {
            jwtProcessor = new DefaultJWTProcessor<>();

            JWKSource<SecurityContext> keySource = JWKSourceBuilder
                    .create(URI.create(keycloakConfig.cert().url()).toURL())
                    .cache(true)
                    .rateLimited(keycloakConfig.request().rateLimit())
                    .build();

            var keySelector = new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, keySource);
            jwtProcessor.setJWSKeySelector(keySelector);

            log.debug("JWT Processor initialized successfully");
        } catch (Exception e) {
            log.error("JWT Processor initialization error", e);
            throw new RuntimeException("JWT Processor initialization error", e);
        }
    }

    public Optional<ClientData> validate(String token) {
        try {
            log.debug("Processing token in keycloak");
            SignedJWT signedJWT = SignedJWT.parse(token);
            JWTClaimsSet claims = jwtProcessor.process(signedJWT, null);
            if (claims.getExpirationTime() == null ||
                    claims.getExpirationTime().getTime() > System.currentTimeMillis()) {

                String clientId = claims.getSubject();
                String preferredUsername = claims.getStringClaim("preferred_username");

                return Optional.of(new ClientData(clientId, preferredUsername));
            }
            return Optional.empty();
        } catch (Exception e) {
            log.warn("Token validation error", e);
            return Optional.empty();
        }
    }
}
