"""Unit tests for MasterClient: focus on the flexlb schedule payload shape,
particularly the api_key pass-through introduced for traffic-policy group routing.
"""

import asyncio
import unittest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import torch

from rtp_llm.config.generate_config import GenerateConfig
from rtp_llm.server.master_client import FlexlbResponse, MasterClient
from rtp_llm.utils.base_model_datatypes import GenerateInput


def _make_generate_input(api_key: Optional[str], seq_len: int = 4) -> GenerateInput:
    token_ids = torch.zeros((seq_len,), dtype=torch.int32)
    gc = GenerateConfig()
    gc.ttft_timeout_ms = 5000
    return GenerateInput(
        request_id=1,
        token_ids=token_ids,
        mm_inputs=[],
        generate_config=gc,
        api_key=api_key,
    )


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class TestMasterClientPayload(unittest.TestCase):
    def _make_client(self, api_key: Optional[str]):
        host_service = MagicMock()
        host_service.get_master_addr.return_value = "10.0.0.1:8080"
        host_service.get_slave_addr = lambda: None

        client = MasterClient(host_service=host_service, master_config=None)

        captured: Dict[str, Any] = {}

        async def fake_send(addr, payload, timeout_ms, request_id):
            captured["addr"] = addr
            captured["payload"] = payload
            return FlexlbResponse.ok_with_result({"code": 200, "server_status": []})

        client._send_schedule_request = fake_send  # type: ignore[assignment]
        return client, captured

    def test_payload_includes_api_key_when_present(self):
        client, captured = self._make_client(api_key="key-vip")
        gen_input = _make_generate_input(api_key="key-vip")

        _run(client.get_backend_role_addrs([1, 2, 3], gen_input, request_id=42))

        payload = captured["payload"]
        self.assertEqual(payload["api_key"], "key-vip")
        self.assertEqual(payload["request_id"], 42)
        self.assertEqual(payload["seq_len"], gen_input.prompt_length)

    def test_payload_omits_api_key_when_none(self):
        client, captured = self._make_client(api_key=None)
        gen_input = _make_generate_input(api_key=None)

        _run(client.get_backend_role_addrs([], gen_input, request_id=7))

        self.assertNotIn("api_key", captured["payload"])

    def test_payload_omits_api_key_when_empty_string(self):
        client, captured = self._make_client(api_key="")
        gen_input = _make_generate_input(api_key="")

        _run(client.get_backend_role_addrs([], gen_input, request_id=8))

        self.assertNotIn("api_key", captured["payload"])

    def test_payload_wire_contract_with_flexlb(self):
        """Wire contract snapshot: flexlb Jackson primary key is ``api_key``
        (snake_case). If this test fails, verify ``@JsonProperty`` on
        org.flexlb.dao.loadbalance.Request has not been renamed.
        """
        import json

        client, captured = self._make_client(api_key="key-contract")
        gen_input = _make_generate_input(api_key="key-contract")

        _run(client.get_backend_role_addrs([42], gen_input, request_id=101))

        body = json.dumps(captured["payload"])
        # Key name must be the snake_case ``api_key`` — that is the primary
        # ``@JsonProperty`` on the flexlb Request DTO.
        self.assertIn('"api_key"', body)
        self.assertIn('"key-contract"', body)
        # Other wire fields the flexlb Request DTO expects.
        for required_key in (
            '"request_id"',
            '"seq_len"',
            '"request_time_ms"',
            '"generate_timeout"',
            '"block_cache_keys"',
        ):
            self.assertIn(required_key, body)


class TestGenerateInputApiKey(unittest.TestCase):
    def test_default_api_key_is_none(self):
        gen_input = GenerateInput(
            request_id=1,
            token_ids=torch.zeros((2,), dtype=torch.int32),
            mm_inputs=[],
            generate_config=GenerateConfig(),
        )
        self.assertIsNone(gen_input.api_key)

    def test_api_key_not_in_repr(self):
        gen_input = GenerateInput(
            request_id=1,
            token_ids=torch.zeros((2,), dtype=torch.int32),
            mm_inputs=[],
            generate_config=GenerateConfig(),
            api_key="secret-token",
        )
        # api_key uses field(repr=False) — must not appear in the dataclass repr
        self.assertNotIn("secret-token", repr(gen_input))
        self.assertNotIn("api_key", repr(gen_input))


if __name__ == "__main__":
    unittest.main()
