"""Unit Tests for RabbitMQ/Pika BrokerClient."""

import itertools
from typing import Any, List, Optional, Tuple
from unittest.mock import MagicMock

import pika  # type: ignore[import]
import pytest
from mqclient import broker_client_manager
from mqclient.broker_client_interface import Message, MQClientException
from mqclient.broker_clients.rabbitmq import HUMAN_PATTERN, _get_credentials, _parse_url
from mqclient.config import (
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
    DEFAULT_TIMEOUT,
    DEFAULT_TIMEOUT_MILLIS,
)

from ...abstract_broker_client_tests.unit_tests import BrokerClientUnitTest


class TestUnitRabbitMQ(BrokerClientUnitTest):
    """Unit test suite interface for RabbitMQ broker_client."""

    broker_client = broker_client_manager.get_broker_client("rabbitmq")
    con_patch = "pika.BlockingConnection"

    @staticmethod
    def _get_nack_mock_fn(mock_con: Any) -> Any:
        """Return mock 'nack' function call."""
        return mock_con.return_value.channel.return_value.basic_nack

    @staticmethod
    def _get_ack_mock_fn(mock_con: Any) -> Any:
        """Return mock 'ack' function call."""
        return mock_con.return_value.channel.return_value.basic_ack

    @staticmethod
    def _get_close_mock_fn(mock_con: Any) -> Any:
        """Return mock 'close' function call."""
        return mock_con.return_value.close

    @staticmethod
    async def _enqueue_mock_messages(
        mock_con: Any, data: List[bytes], ids: List[int], append_none: bool = True
    ) -> None:
        """Place messages on the mock queue."""
        if len(data) != len(ids):
            raise AttributeError("`data` and `ids` must have the same length.")
        messages = [(MagicMock(delivery_tag=i), None, d) for d, i in zip(data, ids)]
        if append_none:
            messages += [(None, None, None)]  # type: ignore
        mock_con.return_value.channel.return_value.consume.return_value.__next__.side_effect = (
            messages
        )

    @pytest.mark.asyncio
    async def test_create_pub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating pub queue."""
        pub = await self.broker_client.create_pub_queue("localhost", queue_name, "")
        assert pub.queue == queue_name  # type: ignore
        mock_con.return_value.channel.assert_called()

    @pytest.mark.asyncio
    async def test_create_sub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating sub queue."""
        sub = await self.broker_client.create_sub_queue(
            "localhost", queue_name, 213, ""
        )
        assert sub.queue == queue_name  # type: ignore
        assert sub.prefetch == 213
        mock_con.return_value.channel.assert_called()

    @pytest.mark.asyncio
    async def test_send_message(self, mock_con: Any, queue_name: str) -> None:
        """Test sending message."""
        pub = await self.broker_client.create_pub_queue("localhost", queue_name, "")
        await pub.send_message(
            b"foo, bar, baz",
            retries=DEFAULT_RETRIES,
            retry_delay=DEFAULT_RETRY_DELAY,
        )
        mock_con.return_value.channel.return_value.basic_publish.assert_called_with(
            exchange="", routing_key=queue_name, body=b"foo, bar, baz"
        )

    @pytest.mark.asyncio
    async def test_get_message(self, mock_con: Any, queue_name: str) -> None:
        """Test getting message."""
        sub = await self.broker_client.create_sub_queue("localhost", queue_name, 1, "")
        mock_con.return_value.is_closed = False  # HACK - manually set attr

        fake_message = (MagicMock(delivery_tag=12), None, Message.serialize("foo, bar"))
        mock_con.return_value.channel.return_value.consume.return_value.__next__.side_effect = [
            fake_message
        ]
        m = await sub.get_message(
            timeout_millis=DEFAULT_TIMEOUT_MILLIS,
            retries=DEFAULT_RETRIES,
            retry_delay=DEFAULT_RETRY_DELAY,
        )
        assert m is not None
        assert m.msg_id == 12
        assert m.data == "foo, bar"

    @pytest.mark.asyncio
    async def test_message_generator_10_upstream_error(
        self, mock_con: Any, queue_name: str
    ) -> None:
        """Failure-test message generator.

        Generator should raise Exception originating upstream (a.k.a.
        from pika-package code).
        """
        sub = await self.broker_client.create_sub_queue("localhost", queue_name, 1, "")
        mock_con.return_value.is_closed = False  # HACK - manually set attr

        retries = 2  # >= 0

        class _MyException(Exception):
            pass

        mock_con.return_value.channel.return_value.consume.return_value.__next__.side_effect = (
            _MyException
        )
        with pytest.raises(_MyException):
            async for m in sub.message_generator(
                timeout=DEFAULT_TIMEOUT,
                propagate_error=True,
                retries=retries,
                retry_delay=DEFAULT_RETRY_DELAY,
            ):
                pass

        # would be called by Queue one more time
        assert self._get_close_mock_fn(mock_con).call_count == retries

        # reset for next call
        self._get_close_mock_fn(mock_con).reset_mock()

        # `propagate_error` attribute has no affect (b/c it deals w/ *downstream* errors)
        mock_con.return_value.channel.return_value.consume.return_value.__next__.side_effect = (
            _MyException
        )
        with pytest.raises(_MyException):
            async for m in sub.message_generator(
                timeout=DEFAULT_TIMEOUT,
                propagate_error=False,
                retries=retries,
                retry_delay=DEFAULT_RETRY_DELAY,
            ):
                pass

        # would be called by Queue one more time
        assert self._get_close_mock_fn(mock_con).call_count == retries


class TestUnitRabbitMQHelpers:
    """Unit test rabbitmq-specific helper functions."""

    def test_000(self) -> None:
        """Sanity check the constants."""
        assert HUMAN_PATTERN == ("[SCHEME://][USER[:PASS]@]HOST[:PORT][/VIRTUAL_HOST]")

    def test_100(self) -> None:
        """Test normal (successful) parsing of `_parse_url()`."""

        def _get_return_tuple(
            subdict: dict, password: Optional[str] = ""
        ) -> Tuple[dict, Optional[str], Optional[str]]:
            return (
                {k: v for k, v in subdict.items() if k not in ["username", "password"]},
                subdict.get("username", None),
                subdict.get("password", password),
            )

        tokens = dict(scheme="wxyz", port=1234, virtual_host="foo", username="hank")
        # test with every number of combinations of `tokens`
        for rlength in range(len(tokens) + 1):
            for _subset in itertools.combinations(tokens.items(), rlength):
                subdict = dict(_subset)

                # host is mandatory
                host = "localhost"
                subdict["host"] = host

                # optional tokens
                if user := subdict.get("username", ""):
                    user = f"{user}@"
                if port := subdict.get("port", ""):
                    port = f":{port}"
                if vhost := subdict.get("virtual_host", ""):
                    vhost = f"/{vhost}"
                if skm := subdict.get("scheme", ""):
                    skm = f"{skm}://"

                address = f"{skm}{user}{host}{port}{vhost}"
                print(address)
                assert _parse_url(address) == _get_return_tuple(subdict, password=None)

                # special optional tokens
                if user:  # password can only be given alongside username
                    subdict["password"] = "secret"
                    address = f"{skm}{subdict['username']}:{subdict['password']}@{host}{port}{vhost}"
                    print(address)
                    assert _parse_url(address) == _get_return_tuple(subdict)

    def test_200(self) -> None:
        """Test `_get_credentials()`."""

        # Case 1: username/password
        cred = pika.credentials.PlainCredentials("username", "password")
        assert _get_credentials("username", "password", "") == cred
        # Sub-case: username/auth_token (override)
        cred = pika.credentials.PlainCredentials("username", "auth_token")
        assert _get_credentials("username", "password", "auth_token") == cred
        assert _get_credentials("username", None, "auth_token") == cred

        # Error: no password for user
        with pytest.raises(MQClientException):
            _get_credentials("username", None, "")

        # Case 2: Only password/token -- Ex: keycloak
        cred = pika.credentials.PlainCredentials("", "password")
        assert _get_credentials(None, "password", "") == cred
        #
        cred = pika.credentials.PlainCredentials("", "auth_token")
        assert _get_credentials(None, "password", "auth_token") == cred

        # Case 3: no auth -- rabbitmq uses guest/guest
        assert _get_credentials(None, None, "") is None
