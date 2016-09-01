package org.zalando.nakadi.service;

import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class CursorTokenService {

    public String generateToken() {
        return UUID.randomUUID().toString();
    }
}
