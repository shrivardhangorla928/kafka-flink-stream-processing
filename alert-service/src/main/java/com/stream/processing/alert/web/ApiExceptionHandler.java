package com.stream.processing.alert.web;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Turns every failure the API can produce into an RFC 7807 {@code application/problem+json} body.
 *
 * <p>One handler rather than per-controller try/catch so the error shape is uniform: the
 * dashboard and the load-test harness both parse these bodies, and a stack trace leaking through
 * a default error page would be both useless to them and a disclosure risk.</p>
 *
 * <p>Client mistakes are reported in full - the caller can fix them. Server failures return an
 * opaque message plus a correlation id that is also written to the log, so an operator can find
 * the stack trace without it ever crossing the network.</p>
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    private static final URI TYPE_NOT_FOUND = URI.create("urn:stream:alert:not-found");
    private static final URI TYPE_VALIDATION = URI.create("urn:stream:alert:validation-failed");
    private static final URI TYPE_BAD_REQUEST = URI.create("urn:stream:alert:bad-request");
    private static final URI TYPE_INTERNAL = URI.create("urn:stream:alert:internal-error");

    @ExceptionHandler(AlertNotFoundException.class)
    public ProblemDetail onNotFound(AlertNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Alert not found");
        problem.setType(TYPE_NOT_FOUND);
        problem.setProperty("alertId", ex.getAlertId());
        return problem;
    }

    /** Body validation: {@code @Valid} on a request record. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail onBodyValidation(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> errors.put(error.getField(), error.getDefaultMessage()));

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "One or more fields are invalid");
        problem.setTitle("Validation failed");
        problem.setType(TYPE_VALIDATION);
        problem.setProperty("errors", errors);
        return problem;
    }

    /** Parameter validation: {@code @Min}/{@code @NotBlank} on a handler argument. */
    @ExceptionHandler(ConstraintViolationException.class)
    public ProblemDetail onParameterValidation(ConstraintViolationException ex) {
        Map<String, String> errors = new LinkedHashMap<>();
        for (ConstraintViolation<?> violation : ex.getConstraintViolations()) {
            errors.put(violation.getPropertyPath().toString(), violation.getMessage());
        }

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "One or more parameters are invalid");
        problem.setTitle("Validation failed");
        problem.setType(TYPE_VALIDATION);
        problem.setProperty("errors", errors);
        return problem;
    }

    /**
     * Unparseable inputs: an unknown enum constant, a malformed instant, a missing parameter or a
     * body that is not JSON. All the caller's fault, all 400.
     */
    @ExceptionHandler({
            MethodArgumentTypeMismatchException.class,
            MissingServletRequestParameterException.class,
            HttpMessageNotReadableException.class,
            InvalidRangeException.class})
    public ProblemDetail onBadRequest(Exception ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, describe(ex));
        problem.setTitle("Bad request");
        problem.setType(TYPE_BAD_REQUEST);
        return problem;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail onUnexpected(Exception ex) {
        String incidentId = UUID.randomUUID().toString();
        log.error("Unhandled failure serving alert API request [incident {}]", incidentId, ex);

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "The request could not be completed. Quote the incident id when reporting it.");
        problem.setTitle("Internal error");
        problem.setType(TYPE_INTERNAL);
        problem.setProperty("incidentId", incidentId);
        return problem;
    }

    private static String describe(Exception ex) {
        if (ex instanceof MethodArgumentTypeMismatchException mismatch) {
            // The raw Jackson/conversion message names internal classes; this one names the
            // parameter the caller actually typed.
            return "Parameter '" + mismatch.getName() + "' has an unusable value: "
                    + mismatch.getValue();
        }
        if (ex instanceof HttpMessageNotReadableException) {
            return "Request body is missing or is not valid JSON";
        }
        return ex.getMessage();
    }
}
