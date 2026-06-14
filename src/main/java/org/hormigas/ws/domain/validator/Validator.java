package org.hormigas.ws.domain.validator;

public interface Validator<T>{
    ValidationResult validate(T obj);
}
