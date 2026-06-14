package org.hormigas.ws.core.feedback;

import java.time.Duration;

public interface Regulator {
    Duration getCurrentIntervalMs();
}
