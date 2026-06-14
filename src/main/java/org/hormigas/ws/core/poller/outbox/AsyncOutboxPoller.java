package org.hormigas.ws.core.poller.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.core.feedback.Regulator;
import org.hormigas.ws.core.poller.AsyncBatchPoller;
import org.hormigas.ws.core.poller.BatchPoller;
import org.hormigas.ws.core.router.publisher.RoutingBackpressurePublisher;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.outbox.OutboxManager;

@Slf4j
@ApplicationScoped
public class AsyncOutboxPoller implements AsyncBatchPoller {

    @Inject
    RoutingBackpressurePublisher publisher;

    @Inject
    Regulator regulator;

    @Inject
    OutboxManager<Message> outboxManager;

    @Inject
    MeterRegistry registry;

    @Inject
    MessengerConfig messengerConfig;

    private Counter errorCounter;
    private BatchPoller delegate;

    @PostConstruct
    public void init() {
        errorCounter = Counter.builder("routing_scheduler_errors_total")
                .description("Number of errors during polling")
                .register(registry);

        delegate = OutboxPoller.builder()
                .outboxManager(outboxManager)
                .registry(registry)
                .publisher(publisher)
                .regulator(regulator)
                .batchSize(messengerConfig.outbound().batchSize())
                .build();
    }

    @Override
    public void poll() {
        delegate.poll()
                .subscribe().with(
                        nothing -> {},
                        failure -> {
                            log.error("Error in scheduled outbox polling", failure);
                            errorCounter.increment();
                        }
                );
    }
}
