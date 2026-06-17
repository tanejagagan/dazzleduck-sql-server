package io.dazzleduck.sql.commons.namedquery;

import io.dazzleduck.sql.common.NamedQueryParameterValidator;
import io.dazzleduck.sql.common.ParameterValidationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Instantiates, caches, and runs {@link NamedQueryParameterValidator} implementations referenced
 * by a named query (by fully-qualified class name). Framework-agnostic — no Flight or templating
 * dependency — so it can be reused by any named-query consumer.
 */
public final class NamedQueryValidators {

    private static final int MAX_VALIDATOR_CACHE_SIZE = 500;

    /** Validator instance cache: fully-qualified class name → singleton instance. */
    private final ConcurrentHashMap<String, NamedQueryParameterValidator> validatorCache = new ConcurrentHashMap<>();

    /**
     * Runs every validator named in {@code validatorClassNames} against {@code parameters},
     * aggregating all failures into a single {@link ParameterValidationException}. A {@code null}
     * or empty array is a no-op.
     */
    public void validate(String[] validatorClassNames, Map<String, String> parameters)
            throws ParameterValidationException {
        if (validatorClassNames == null || validatorClassNames.length == 0) {
            return;
        }
        Map<String, String> safeParams = parameters != null ? parameters : Map.of();
        List<String> errors = new ArrayList<>();
        for (String className : validatorClassNames) {
            try {
                getOrCreateValidator(className).validate(safeParams);
            } catch (ParameterValidationException e) {
                errors.add(e.getMessage());
            } catch (RuntimeException e) {
                errors.add("Validator configuration error: " + className + " — " + e.getMessage());
            }
        }
        if (!errors.isEmpty()) {
            throw new ParameterValidationException(String.join("; ", errors));
        }
    }

    /**
     * Returns the human-readable {@link NamedQueryParameterValidator#description()} for each validator
     * class, falling back to the class name when a validator cannot be instantiated. Useful for
     * exposing a named query's parameter contract without leaking implementation class names.
     */
    public List<String> describe(String[] validatorClassNames) {
        if (validatorClassNames == null || validatorClassNames.length == 0) {
            return List.of();
        }
        List<String> descriptions = new ArrayList<>(validatorClassNames.length);
        for (String className : validatorClassNames) {
            try {
                descriptions.add(getOrCreateValidator(className).description());
            } catch (RuntimeException e) {
                descriptions.add(className);
            }
        }
        return descriptions;
    }

    private NamedQueryParameterValidator getOrCreateValidator(String className) {
        NamedQueryParameterValidator cached = validatorCache.get(className);
        if (cached != null) {
            return cached;
        }
        NamedQueryParameterValidator instance = instantiateValidator(className);
        if (validatorCache.size() < MAX_VALIDATOR_CACHE_SIZE) {
            NamedQueryParameterValidator existing = validatorCache.putIfAbsent(className, instance);
            return existing != null ? existing : instance;
        }
        return instance;
    }

    private NamedQueryParameterValidator instantiateValidator(String className) {
        try {
            return (NamedQueryParameterValidator) Class.forName(className)
                    .getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate validator: " + className, e);
        }
    }
}
