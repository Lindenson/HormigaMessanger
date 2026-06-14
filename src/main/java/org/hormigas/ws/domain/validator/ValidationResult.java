package org.hormigas.ws.domain.validator;

import java.util.List;

public record ValidationResult(List<String> errors) {
    public boolean isValid() {
        return errors == null || errors.isEmpty();
    }

    public static ValidationResult of(List<String> errors) {
        return new ValidationResult(List.copyOf(errors));
    }
}
