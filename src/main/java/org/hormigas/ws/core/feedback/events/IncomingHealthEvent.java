package org.hormigas.ws.core.feedback.events;

public record IncomingHealthEvent(boolean droppedDetected, int droppedCount) {}
