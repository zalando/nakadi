package org.zalando.nakadi.plugin.auth;

import com.google.common.cache.LoadingCache;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class MerchantGatewayService {
    private static final Logger LOG = LoggerFactory.getLogger(MerchantGatewayService.class);

    private final LoadingCache<String, PublicKey> pubkeyCache;
    private static final int READ_TIMEOUT = 1000;
    private static final int CONNECT_TIMEOUT = 1000;

    public MerchantGatewayService(final LoadingCache<String, PublicKey> pubkeyCache) {
        this.pubkeyCache = pubkeyCache;
    }

    public boolean isSignatureValid(final XConsumer xConsumer) {
        return Optional.ofNullable(xConsumer.getKeyId())
                .map(keyId -> {
                    try {
                        return pubkeyCache.get(keyId);
                    } catch (final ExecutionException e) {
                        LOG.error("Failed to retrieve public key for signature verification", e);
                        throw new RuntimeException("Authorization failed");
                    }
                })
                .map(pubkey -> {
                    try {
                        return verifyContent(xConsumer.getContent(), xConsumer.getSignature(), pubkey);
                    } catch (InvalidKeyException | SignatureException e) {
                        LOG.error("Invalid signature values", e);
                        return false;
                    } catch (final NoSuchAlgorithmException e) {
                        LOG.error("Failed to verify signature", e);
                        throw new RuntimeException("Authorization failed");
                    }
                })
                .orElse(false);
    }

    // Adapted from the example at https://merchant-platform.docs.zalando.net/components/iam-examples/
    private boolean verifyContent(final String content, final String signatureString, final PublicKey publicKey) throws
            InvalidKeyException, SignatureException, NoSuchAlgorithmException {
        if (signatureString == null) {
            return false;
        }
        final Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initVerify(publicKey);

        final Base64.Decoder decoder = Base64.getUrlDecoder();
        final byte[] signatureBytes = decoder.decode(signatureString);

        signature.update(content.getBytes(Charset.forName("UTF-8")));

        return signature.verify(signatureBytes);
    }

    static PublicKey loadPublicKey(final String url) throws IOException, NoSuchAlgorithmException,
            InvalidKeySpecException {
        // getting the public key from the gateway endpoint
        final URL publicKeyUrl = new URL(url);
        final HttpURLConnection huc = (HttpURLConnection) publicKeyUrl.openConnection();
        huc.setConnectTimeout(CONNECT_TIMEOUT);
        huc.setReadTimeout(READ_TIMEOUT);
        huc.setRequestMethod("GET");
        huc.connect();
        try (InputStream stream = huc.getInputStream();
             InputStreamReader inputStreamReader = new InputStreamReader(stream);
             BufferedReader reader = new BufferedReader(inputStreamReader)) {
            final String pubKey = reader.lines()
                    // unfortunately PEM reader is not a part of JDK, so we have to remove meta data
                    .filter(str -> !str.equals("-----BEGIN PUBLIC KEY-----"))
                    .filter(str -> !str.equals("-----END PUBLIC KEY-----"))
                    .collect(Collectors.joining("\n"));

            final Base64.Decoder decoder = Base64.getMimeDecoder();
            final byte[] keyBytes = decoder.decode(pubKey);

            // parse and initialize the public key
            final X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
            final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePublic(spec);
        } finally {
            huc.disconnect();
        }
    }

    /**
     * Verifies if the contents of the base64 non padded string provided as X-Consumer contains a field named `bpids`
     * and that its value is of type JSONArray and it contains minimum of 1 element.
     *
     * @param  xConsumer object containing header fields as provided by the Gateway (not decoded)
     * @return true if all conditions are met. False otherwise.
     */
    public boolean isValidJsonWhenDecoded(final XConsumer xConsumer) {
        try {
            final String decodedXConsumer = new String(Base64.getDecoder().decode(xConsumer.getContent()));
            final JSONObject jsonObject = new JSONObject(decodedXConsumer);

            if (jsonObject.getJSONArray("bpids").length() >= 1) {
                return true;
            } else {
                LOG.warn("Empty X-Consumer bpids array {}", decodedXConsumer);
            }
        } catch (JSONException e) {
            LOG.warn("Decoded X-Consumer header content is not valid", e);
        } catch (IllegalArgumentException e) {
            LOG.warn("Provided X-Consumer header content could not be decoded");
        }
        return false;
    }
}
