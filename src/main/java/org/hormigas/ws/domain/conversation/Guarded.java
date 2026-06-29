package org.hormigas.ws.domain.conversation;

/** A membership-guarded result: the {@link Outcome} plus a value (non-null only on OK). */
public record Guarded<T>(Outcome outcome, T value) {
    public static <T> Guarded<T> ok(T value) { return new Guarded<>(Outcome.OK, value); }
    public static <T> Guarded<T> notFound() { return new Guarded<>(Outcome.NOT_FOUND, null); }
    public static <T> Guarded<T> forbidden() { return new Guarded<>(Outcome.FORBIDDEN, null); }
}
