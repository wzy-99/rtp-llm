"""Unit tests for extract_api_key header parsing."""

import unittest

from rtp_llm.server.request_headers import extract_api_key


class TestExtractApiKey(unittest.TestCase):
    def test_returns_none_when_headers_is_none(self):
        self.assertIsNone(extract_api_key(None))

    def test_returns_none_when_no_relevant_header(self):
        self.assertIsNone(extract_api_key({"Content-Type": "application/json"}))

    def test_x_api_key_wins(self):
        headers = {
            "X-Api-Key": "key-xapi",
            "Api-Key": "key-api",
            "Authorization": "Bearer key-bearer",
        }
        self.assertEqual(extract_api_key(headers), "key-xapi")

    def test_api_key_when_no_x_api_key(self):
        headers = {
            "Api-Key": "key-api",
            "Authorization": "Bearer key-bearer",
        }
        self.assertEqual(extract_api_key(headers), "key-api")

    def test_bearer_when_no_explicit_api_key(self):
        headers = {"Authorization": "Bearer key-bearer"}
        self.assertEqual(extract_api_key(headers), "key-bearer")

    def test_bearer_trimmed(self):
        headers = {"Authorization": "Bearer   key-bearer  "}
        self.assertEqual(extract_api_key(headers), "key-bearer")

    def test_authorization_without_bearer_prefix_ignored(self):
        headers = {"Authorization": "Basic dXNlcjpwYXNz"}
        self.assertIsNone(extract_api_key(headers))

    def test_bearer_with_empty_token(self):
        headers = {"Authorization": "Bearer    "}
        self.assertIsNone(extract_api_key(headers))

    def test_blank_x_api_key_falls_through(self):
        headers = {"X-Api-Key": "   ", "Api-Key": "key-api"}
        self.assertEqual(extract_api_key(headers), "key-api")

    def test_strips_whitespace(self):
        headers = {"X-Api-Key": "  padded-key  "}
        self.assertEqual(extract_api_key(headers), "padded-key")


if __name__ == "__main__":
    unittest.main()
