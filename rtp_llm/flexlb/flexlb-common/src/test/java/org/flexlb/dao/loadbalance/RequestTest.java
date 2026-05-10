package org.flexlb.dao.loadbalance;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

class RequestTest {

    @Test
    void should_not_include_api_key_in_to_string() {
        Request request = new Request();
        request.setRequestId(12345L);
        request.setApiKey("secret-api-key");

        assertFalse(request.toString().contains("secret-api-key"));
    }
}
