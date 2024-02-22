package org.zalando.nakadi.plugin.auth;

import com.google.common.cache.LoadingCache;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MerchantGatewayServiceTest {

    private final LoadingCache pubkeyCache = mock(LoadingCache.class);
    private final MerchantGatewayService service = new MerchantGatewayService(pubkeyCache);

    @Test
    public void testVerifySignature() throws ExecutionException, NoSuchAlgorithmException, InvalidKeySpecException {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        // data generate using binary tool provided by
        // https://merchant-platform.docs.zalando.net/components/iam/#x-consumer-dev-tool:
        //
        // ./osx.x-consumer header --scopes posts/read,posts/write --bpids ahzhd657-dhsdjs-dshd83-dhsdjs
        // ./osx.x-consumer signature --scopes posts/read,posts/write --bpids ahzhd657-dhsdjs-dshd83-dhsdjs
        // ./osx.x-consumer key
        when(request.getHeader("X-Consumer")).thenReturn("eyJjbGllbnRfaWQiOiIiLCJncm91cHMiOlsiIl0sInNjb3BlcyI6" +
                "WyJwb3N0cy9yZWFkIiwicG9zdHMvd3JpdGUiXSwiYnBpZHMiOlsiYWh6aGQ2NTctZGhzZGpzLWRzaGQ4My1kaHNkanMiXX0");
        when(request.getHeader("X-Consumer-Signature")).thenReturn("zcUFED_zrGtsoNFQIB9wfk2eEi9mlQpjUkQMbOeJLY" +
                "kTN2Ky4XEd9Vql1x-YCH1VlmaP9d1r3PvSX3dEZlmidD3e-CqRKhekIfH6K9YGz999eqvMy4-2l4NJnHDg_29Caw78ilkyUJcK-" +
                "vLGCIeLSkzOo_3wRCwOTJHCEf3vvdJ0MXxq9P4FVq97v9X3deGDXEMemMT4XjkAvlnm43ruhgJEx3pDCiVF_U1NvSVm4S1hECiL" +
                "0tUU5gwCiBZtXv7v5_xv4p1Y-Y2Vza1JBoxNXeNj62PSu22R9Dmap2fZrWSwh3fLmsaKCldEKfwAvt5-PcpG20Xvual6naiVMfg" +
                "Ntw7xLv5jgzAiCLn711unRrgx0hidf4tNMUVh3xkf5vudJWAbeyEiPTEiTijF62CKL2m7hCBpR41EBA-DsVdImJZC6mACCTDube" +
                "-QUYa8N6lOYBK6pXWNuaEE0mLC21LaMGc902Ma3JO_INUCy7y2Ud_1mUXpafsaI0OxFYKMCzq95cFLgqcIDFDTIRzs05n3p4flm" +
                "VAG1D1DpSOiuHhwGxqI4vyngOx6cM_Eua5nPzP8lBz7JY4OvIvzolGFUiOMRGrgN9PSbMn7OqHWKIOMrPmlwdSDMHk7LPdsOtU0" +
                "FX9lo1xSy3RC_Z_gBKt56Zp9ODRqVhL7iG5vq44V8dkBzG0");
        when(request.getHeader("X-Consumer-Key-ID")).thenReturn("pub-key-id");

        final String pubKey = "MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAzknBJeLSAO9hgN2Si160\n" +
                "eGk+EI5p+ktvowyrt2PkQhwBBkkgLrqqj0Na0Rvs/87upYQeSLmeHQ1LWzZhAfWS\n" +
                "b99x2KsBHzt//gSpYkokHv0k/aGVbNrgfkKxkKkJYLzfxLqBMIduAW1mKRgKYtKN\n" +
                "8QSoD00533KNo3KFGSyOD5N9WNXR18npJni4+atiHvxf5PYK5KZOAWrNHybS3+uO\n" +
                "xYUCg/rWNdT+St5FdfqsuKoAjsypIOQscWZjDOqU6+bUimnKKtUs3zt/+aDn/po0\n" +
                "ioR/y6X+1RmnNj0RoC3O1YRnun7Mwe66YfGuF/1gSF8i2Ok2zltw9yWEvVH3Lbp1\n" +
                "JTh+mgvRnf8ThA3EGBRTtEo8pyGto1JoQsekHTK1dY9U+BCH7FvM10NDVHCS6ea9\n" +
                "Vk2vXmTBnR+o4rQ0nSii4Q97Pt1KcD/jY1611+u8gWe2jz+eZsczTopKq5inAh4+\n" +
                "ZZ8JPyCcTxf473rU+7aD/zDIB27Lz02/b2hyKAoOZm0hCNenTBRuOaPWa9dWJ5s4\n" +
                "/gbBroJnkmbVarrhmdu1wf0/StEJWLiVG8Bv7j6s3DRyq6D+urAVUkccC/Vv23jz\n" +
                "w611ykkLeNT8FRo+Cgcw8xVbYrYUiI5H+AGfCY5TM8RKfzEW5cHgtz0+XFTT3vm/\n" +
                "LQDuDGrXHsNeSQG25/WTy0UCAwEAAQ==\n";

        final Base64.Decoder decoder = Base64.getMimeDecoder();
        final byte[] keyBytes = decoder.decode(pubKey);
        final X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
        final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        final PublicKey publicKey = keyFactory.generatePublic(spec);

        when(pubkeyCache.get("pub-key-id")).thenReturn(publicKey);

        final XConsumer consumer = XConsumer.fromRequest(request);
        assertTrue(service.isSignatureValid(consumer));
    }

    @Test
    public void testIsValidJsonWhenDecoded() {
        final HttpServletRequest request = mockXConsumer("{\"client_id\":\"\",\"groups\":[\"\"]," +
                "\"scopes\":[\"posts/read\",\"posts/write\"]," +
                "\"bpids\":[\"ahzhd657-dhsdjs-dshd83-dhsdjs\"]}");

        final XConsumer consumer = XConsumer.fromRequest(request);
        assertTrue(service.isValidJsonWhenDecoded(consumer));
    }

    @Test
    public void testIsNotValidJsonWhenDecoded() {
        final HttpServletRequest request = mockXConsumer("{\"client_id\":\"\",\"groups\":[\"\"]," +
                "\"scopes\":[\"posts/read\",\"posts/write\"]," +
                "\"bpids\":[\"ahzhd657-dhsdjs-dshd83-dhsdjs");

        final XConsumer consumer = XConsumer.fromRequest(request);
        assertFalse(service.isValidJsonWhenDecoded(consumer));
    }

    @Test
    public void testRejectedWhenZeroBpids() {
        final HttpServletRequest request = mockXConsumer("{\"client_id\":\"\",\"groups\":[\"\"]," +
                "\"scopes\":[\"posts/read\",\"posts/write\"]," +
                "\"bpids\":[]}");

        final XConsumer consumer = XConsumer.fromRequest(request);
        assertFalse(service.isValidJsonWhenDecoded(consumer));
    }

    @Test
    public void testRejectedWhenBpidsNotArray() {
        final HttpServletRequest request = mockXConsumer("{\"client_id\":\"\",\"groups\":[\"\"]," +
                "\"scopes\":[\"posts/read\",\"posts/write\"]," +
                "\"bpids\":{}}");

        final XConsumer consumer = XConsumer.fromRequest(request);
        assertFalse(service.isValidJsonWhenDecoded(consumer));
    }

    private HttpServletRequest mockXConsumer(final String content) {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        final String encodedString = Base64.getEncoder().withoutPadding().encodeToString(content.getBytes());
        when(request.getHeader("X-Consumer")).thenReturn(encodedString);

        return request;
    }
}