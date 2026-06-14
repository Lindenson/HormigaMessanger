package org.hormigas.ws.scheduler;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.poller.AsyncBatchPoller;

@Slf4j
@ApplicationScoped
@IfBuildProfile("prod")
public class RoutingScheduler {

    @Inject
    AsyncBatchPoller asyncBatchPoller;

    @Scheduled(every = "${processing.messages.outbound.polling-ms}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void pollOutbox() {
        asyncBatchPoller.poll();
    }
}
