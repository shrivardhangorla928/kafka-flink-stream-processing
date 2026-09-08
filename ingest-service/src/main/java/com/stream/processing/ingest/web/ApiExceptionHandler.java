package com.stream.processing.ingest.web;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.validation.method.ParameterValidationResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns every failure this service can produce into an RFC 7807 problem document.
 *
 * <p>Spring's {@link ProblemDetail} is used rather than a bespoke error record so the responses
 * are the same shape as the ones the alert service returns, and so the {@code application/
 * problem+json} content type tells a client it is safe to parse machine-readably. Validation
 * failures additionally carry a field-to-message map, because a caller posting a 500-reading
 * batch needs to know which reading was wrong, not merely that one was.</p>
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandler.class);

    private static final URI VALIDATION_TYPE = URI.create("urn:stream:problem:validation-failed");
    private static final URI MALFORMED_TYPE = URI.create("urn:stream:problem:malformed-request");
    private static final URI SIMULATOR_TYPE = URI.create("urn:stream:problem:simulator-disabled");
    private static final URI INTERNAL_TYPE = URI.create("urn:stream:problem:internal-error");

    private static final String ERRORS_PROPERTY = "errors";
    private static final String TIMESTAMP_PROPERTY = "timestamp";

    /** Body-level bean validation failures, i.e. {@code POST /readings} with a bad payload. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail onInvalidBody(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new LinkedHashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            errors.put(error.getField(), error.getDefaultMessage());
        }
        ex.getBindingResult().getGlobalErrors()
                .forEach(error -> errors.put(error.getObjectName(), error.getDefaultMessage()));

        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, VALIDATION_TYPE, "Validation failed",
                "The reading was rejected because " + errors.size() + " field(s) are invalid");
        problem.setProperty(ERRORS_PROPERTY, errors);
        return problem;
    }

    /**
     * Element-level failures raised by method validation, which is how a bad entry inside a
     * {@code List<@Valid ...>} batch surfaces.
     */
    @ExceptionHandler(HandlerMethodValidationException.class)
    public ProblemDetail onInvalidParameters(HandlerMethodValidationException ex) {
        List<String> messages = new ArrayList<>();
        for (ParameterValidationResult result : ex.getAllValidationResults()) {
            result.getResolvableErrors().forEach(error -> messages.add(error.getDefaultMessage()));
        }

        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, VALIDATION_TYPE, "Validation failed",
                "The request was rejected because " + messages.size() + " constraint(s) are violated");
        problem.setProperty(ERRORS_PROPERTY, messages);
        return problem;
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ProblemDetail onConstraintViolation(ConstraintViolationException ex) {
        Map<String, String> errors = new LinkedHashMap<>();
        for (ConstraintViolation<?> violation : ex.getConstraintViolations()) {
            errors.put(String.valueOf(violation.getPropertyPath()), violation.getMessage());
        }
        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, VALIDATION_TYPE, "Validation failed",
                "The request violates " + errors.size() + " constraint(s)");
        problem.setProperty(ERRORS_PROPERTY, errors);
        return problem;
    }

    /** Unparseable JSON, a wrong type, or an unknown enum constant such as {@code sensorType=SNOW}. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail onUnreadableBody(HttpMessageNotReadableException ex) {
        LOG.debug("Rejected unreadable request body", ex);
        return problem(HttpStatus.BAD_REQUEST, MALFORMED_TYPE, "Malformed request",
                "The request body could not be parsed as a telemetry reading");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail onIllegalArgument(IllegalArgumentException ex) {
        return problem(HttpStatus.BAD_REQUEST, VALIDATION_TYPE, "Invalid request", ex.getMessage());
    }

    @ExceptionHandler(SimulatorDisabledException.class)
    public ProblemDetail onSimulatorDisabled(SimulatorDisabledException ex) {
        return problem(HttpStatus.SERVICE_UNAVAILABLE, SIMULATOR_TYPE, "Simulator disabled", ex.getMessage());
    }

    /**
     * Last resort. The message is deliberately generic - a broker failure's stack trace belongs in
     * the log, not in a response body - while the log entry keeps the detail.
     */
    @ExceptionHandler(Exception.class)
    public ProblemDetail onUnexpected(Exception ex) {
        LOG.error("Unhandled failure while serving a telemetry request", ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, INTERNAL_TYPE, "Internal error",
                "The reading could not be accepted; see the ingest service logs");
    }

    private static ProblemDetail problem(HttpStatus status, URI type, String title, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(type);
        problem.setTitle(title);
        problem.setProperty(TIMESTAMP_PROPERTY, Instant.now());
        return problem;
    }
}
