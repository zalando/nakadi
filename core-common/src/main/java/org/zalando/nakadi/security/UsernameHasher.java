package org.zalando.nakadi.security;

import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;

@Component
public class UsernameHasher {

    private final byte[] salt;
    private final ThreadLocal<MessageDigest> messageDigestThreadLocal;

    @Autowired
    public UsernameHasher(@Value("${nakadi.hasher.salt}") final String salt) {
        this.salt = salt.getBytes(Charsets.UTF_8);
        this.messageDigestThreadLocal = ThreadLocal.withInitial(DigestUtils::getSha256Digest);
    }

    public String hash(final String value) {
        final MessageDigest messageDigest = messageDigestThreadLocal.get();
        messageDigest.reset();
        messageDigest.update(salt);
        messageDigest.update(value.getBytes(Charsets.UTF_8));
        return Hex.encodeHexString(messageDigest.digest());
    }
}
