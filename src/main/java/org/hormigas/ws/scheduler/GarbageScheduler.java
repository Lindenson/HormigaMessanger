package org.hormigas.ws.scheduler;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.garbage.AsyncGarbageCollector;

@Slf4j
@ApplicationScoped
@IfBuildProfile("prod")
public class GarbageScheduler {

    @Inject
    AsyncGarbageCollector collector;

    @Scheduled(every = "${processing.messages.collector.every-s}",
    concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void run() {
        collector.collect();
    }
}
