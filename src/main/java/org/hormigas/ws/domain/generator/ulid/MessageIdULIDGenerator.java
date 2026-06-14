package org.hormigas.ws.domain.generator.ulid;

import com.github.f4b6a3.ulid.UlidCreator;
import jakarta.enterprise.context.ApplicationScoped;
import org.hormigas.ws.domain.generator.IdGenerator;

@ApplicationScoped
public class MessageIdULIDGenerator implements IdGenerator {
    @Override
    public String generateId() {
        return UlidCreator.getUlid().toString();
    }
}
