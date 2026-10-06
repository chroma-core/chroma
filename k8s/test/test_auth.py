"""HTTP integration test against the Tilt-managed frontend and auth service.

Run after Tilt's auth bootstrap is ready: python3 k8s/test/test_auth.py
Only Python's standard library is required.
"""

import json
import os
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class TiltAuthTest(unittest.TestCase):
    endpoint = os.environ.get("CHROMA_TEST_FRONTEND_URL", "http://localhost:8000")
    key = os.environ.get("CHROMA_TEST_API_KEY", "tilt-test-api-key")

    def request(self, path, key=None):
        headers = {} if key is None else {"x-chroma-token": key}
        request = Request(self.endpoint + path, headers=headers)
        try:
            with urlopen(request, timeout=10) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            with error:
                return error.code, error.read().decode()

    def test_healthcheck_remains_public(self):
        self.assertEqual(self.request("/api/v2/healthcheck")[0], 200)

    def test_missing_and_invalid_keys_are_denied(self):
        for path in [
            "/api/v2/auth/identity",
            "/api/v2/tenants/default_tenant",
            "/api/v2/tenants/default_tenant/databases/default_database/collections",
        ]:
            for key in [None, "invalid-key", "tilt-service-api-key"]:
                with self.subTest(path=path, key=key):
                    self.assertEqual(self.request(path, key)[0], 401)

    def test_valid_key_and_bootstrapped_resources(self):
        status, identity = self.request("/api/v2/auth/identity", self.key)
        self.assertEqual(status, 200)
        self.assertEqual(identity["tenant"], "default_tenant")
        for path, name in [
            ("/api/v2/tenants/default_tenant", "default_tenant"),
            ("/api/v2/tenants/default_tenant/databases/default_database", "default_database"),
        ]:
            with self.subTest(path=path):
                status, body = self.request(path, self.key)
                self.assertEqual(status, 200)
                self.assertEqual(body["name"], name)
        self.assertEqual(self.request(
            "/api/v2/tenants/default_tenant/databases/default_database/collections", self.key
        )[0], 200)

    def test_valid_key_cannot_access_another_tenant(self):
        self.assertEqual(self.request("/api/v2/tenants/another_tenant", self.key)[0], 403)


if __name__ == "__main__":
    unittest.main(verbosity=2)
