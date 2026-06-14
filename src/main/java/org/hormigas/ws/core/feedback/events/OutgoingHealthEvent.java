package org.hormigas.ws.core.feedback.events;

public record OutgoingHealthEvent(boolean droppedDetected, int droppedCount) {}
