package com.listings.infra.dedup.utils;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public class Utils {

    public static String generateHash(final String value) {
        final String hash = Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString();
        return hash;
    }

}
