package org.zalando.nakadi.plugin.auth;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedCountStrategy;
import org.echocat.jomon.runtime.concurrent.Retryer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Open Policy Agent client
 */
public class OPAClient {
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private final OkHttpClient httpClient;
    private final TokenProvider provider;
    private final String opaUri;
    private final long retryTimeout;
    private final int retryTimes;
    private final OpaDegradationPolicy opaDegradationPolicy;

    public OPAClient(final OkHttpClient httpClient,
                     final TokenProvider provider,
                     final String endpoint,
                     final String policyPath,
                     final long retryTimeout,
                     final int retryTimes,
                     final OpaDegradationPolicy opaDegradationPolicy) {

        this.httpClient = httpClient;
        this.provider = provider;
        this.opaUri = endpoint + policyPath;
        this.retryTimeout = retryTimeout;
        this.retryTimes = retryTimes;
        this.opaDegradationPolicy = opaDegradationPolicy;
    }

    public Set<String> getRetailerIdsForService(final String uid) {
        return getRetailerIds(new JSONObject().put("service", new JSONObject().put("kio_app_id", uid)));
    }

    public Set<String> getRetailerIdsForUser(final String uid) {
        return getRetailerIds(new JSONObject().put("user", new JSONObject().put("ldap_name", uid)));
    }

    private static class RetryException extends RuntimeException {
        RetryException(final String message) {
            super(message);
        }
    }

    private Set<String> getRetailerIds(final JSONObject input) {
        final Span span = GlobalTracer.get().buildSpan("get_retailer_ids").start();
        try (Closeable ignored = GlobalTracer.get().activateSpan(span)) {

            return getRetailerIdsInternal(input);

        } catch (final IOException ioe) {
            throw new RuntimeException("Exception while closing span scope", ioe);
        } catch (final RuntimeException re) {
            span.setTag(Tags.ERROR, true);
            return opaDegradationPolicy.handle(input.toString(), re);
        } finally {
            span.finish();
        }
    }

    private Set<String> getRetailerIdsInternal(final JSONObject input) {
        final JSONObject opaInput = new JSONObject().put("input", input);
        final RequestBody body = RequestBody.create(opaInput.toString(), JSON);

        final Request request = new Request.Builder()
                .url(opaUri)
                .addHeader("Accept", "application/json")
                .addHeader("Authorization", "Bearer " + provider.getToken())
                .post(body)
                .build();

        final String opaResponse = Retryer.executeWithRetry(() -> {
            try (Response response = httpClient.newCall(request).execute()) {
                final String responseBodyString;
                try (ResponseBody responseBody = response.body()) {
                    responseBodyString = responseBody.string();
                }

                if (response.code() != 200) {
                    if (response.code() >= 500) {
                        throw new RetryException("Expected to have status code 200, but got " + response.code() +
                                " with body " + responseBodyString);
                    } else {
                        throw new RuntimeException(
                                "Non-retryable status code " + response.code() +
                                        " received while executing " + opaInput +
                                        ", response is: " + responseBodyString);
                    }
                } else {
                    return responseBodyString;
                }
            }
        }, new RetryForSpecifiedCountStrategy(retryTimes)
                .withWaitBetweenEachTry(retryTimeout)
                .withExceptionsThatForceRetry(IOException.class, RetryException.class));

        try {
            return parseOPAResponse(opaResponse);
        } catch (final JSONException jpe) {
            throw new RuntimeException("Unable to parse OPA response; request=" + opaInput +
                    "; response=" + opaResponse, jpe);
        }
    }

    private Set<String> parseOPAResponse(final String responseBody) throws JSONException {
        final JSONArray retailerIds = new JSONObject(responseBody)
                .getJSONObject("result")
                .getJSONObject("decision")
                .getJSONArray("retailer_ids");
        return retailerIds.toList().stream().map(Object::toString).collect(Collectors.toSet());
    }
}
