"""Run integration tests for given broker_client, on broker_client_interface
classes.

Verify functionality that is abstracted away from the Queue class.
"""

# pylint:disable=invalid-name,too-many-public-methods,redefined-outer-name,unused-import

import copy
import itertools
import logging
from typing import List, Optional

import asyncstdlib as asl
import pytest
from mqclient.broker_client_interface import BrokerClient, Message
from mqclient.config import DEFAULT_RETRIES, DEFAULT_RETRY_DELAY, DEFAULT_TIMEOUT_MILLIS

from .utils import DATA_LIST, _log_recv, _log_send


def _log_recv_message(recv_msg: Optional[Message]) -> None:
    recv_data = None
    if recv_msg:
        recv_data = recv_msg.data
    _log_recv(f"{recv_msg} -> {recv_data}")


class PubSubBrokerClientInterface:
    """Integration test suite for broker_client_interface objects.

    Only test things that cannot be tested via the Queue class.
    """

    broker_client: BrokerClient
    timeout = 1

    @pytest.mark.asyncio
    async def test_00(self, queue_name: str, auth_token: str) -> None:
        """Sanity test."""
        pub = await self.broker_client.create_pub_queue(
            "localhost", queue_name, auth_token
        )
        sub = await self.broker_client.create_sub_queue(
            "localhost", queue_name, 1, auth_token
        )

        # send
        for msg in DATA_LIST:
            raw_data = Message.serialize(msg)
            await pub.send_message(
                raw_data,
                retries=DEFAULT_RETRIES,
                retry_delay=DEFAULT_RETRY_DELAY,
            )
            _log_send(msg)

        # receive
        for i in itertools.count():
            logging.info(i)
            assert i <= len(DATA_LIST)

            recv_msg = await sub.get_message(
                timeout_millis=DEFAULT_TIMEOUT_MILLIS,
                retries=DEFAULT_RETRIES,
                retry_delay=DEFAULT_RETRY_DELAY,
            )
            _log_recv_message(recv_msg)

            # check received message
            if i == len(DATA_LIST):
                assert not recv_msg  # None signifies end of stream
                break

            assert recv_msg
            assert DATA_LIST[i] == recv_msg.data

            await sub.ack_message(
                recv_msg,
                retries=DEFAULT_RETRIES,
                retry_delay=DEFAULT_RETRY_DELAY,
            )

        await pub.close()
        await sub.close()

    @pytest.mark.asyncio
    async def test_10(self, queue_name: str, auth_token: str) -> None:
        """Test nacking, front-loaded sending.

        Order is not guaranteed on redelivery.
        """
        pub = await self.broker_client.create_pub_queue(
            "localhost", queue_name, auth_token
        )
        sub = await self.broker_client.create_sub_queue(
            "localhost", queue_name, 1, auth_token
        )

        # send
        for msg in DATA_LIST:
            raw_data = Message.serialize(msg)
            await pub.send_message(
                raw_data,
                retries=DEFAULT_RETRIES,
                retry_delay=DEFAULT_RETRY_DELAY,
            )
            _log_send(msg)

        # receive -- nack each message, once, and anticipate its redelivery
        nacked_msgs: List[Message] = []
        redelivered_msgs: List[Message] = []
        for i in itertools.count():
            logging.info(i)
            assert i < len(DATA_LIST) * 10  # large enough but avoids inf loop

            # all messages have been acked and redelivered
            if len(redelivered_msgs) == len(DATA_LIST):
                redelivered_data = [m.data for m in redelivered_msgs]
                assert all((d in DATA_LIST) for d in redelivered_data)
                break

            recv_msg = await sub.get_message(
                timeout_millis=DEFAULT_TIMEOUT_MILLIS,
                retries=DEFAULT_RETRIES,
                retry_delay=DEFAULT_RETRY_DELAY,
            )
            _log_recv_message(recv_msg)

            if not recv_msg:
                logging.info("waiting...")
                continue
            assert recv_msg.data in DATA_LIST

            # message was redelivered, so ack it
            if recv_msg in nacked_msgs:
                logging.info("REDELIVERED!")
                nacked_msgs.remove(recv_msg)
                redelivered_msgs.append(recv_msg)
                await sub.ack_message(
                    recv_msg,
                    retries=DEFAULT_RETRIES,
                    retry_delay=DEFAULT_RETRY_DELAY,
                )
            # otherwise, nack message
            else:
                nacked_msgs.append(recv_msg)
                await sub.reject_message(
                    recv_msg,
                    retries=DEFAULT_RETRIES,
                    retry_delay=DEFAULT_RETRY_DELAY,
                )
                logging.info("NACK!")

        await pub.close()
        await sub.close()

    @pytest.mark.asyncio
    async def test_11(self, queue_name: str, auth_token: str) -> None:
        """Test nacking, mixed sending and receiving.

        Order is not guaranteed on redelivery.
        """
        pub = await self.broker_client.create_pub_queue(
            "localhost", queue_name, auth_token
        )
        sub = await self.broker_client.create_sub_queue(
            "localhost", queue_name, 1, auth_token
        )

        data_to_send = copy.deepcopy(DATA_LIST)
        nacked_msgs: List[Message] = []
        redelivered_msgs: List[Message] = []
        for i in itertools.count():
            logging.info(i)
            assert i < len(DATA_LIST) * 10  # large enough but avoids inf loop

            # all messages have been acked and redelivered
            if len(redelivered_msgs) == len(DATA_LIST):
                redelivered_data = [m.data for m in redelivered_msgs]
                assert all((d in DATA_LIST) for d in redelivered_data)
                break

            # send a message
            if data_to_send:
                msg = data_to_send[0]
                raw_data = Message.serialize(msg)
                await pub.send_message(
                    raw_data,
                    retries=DEFAULT_RETRIES,
                    retry_delay=DEFAULT_RETRY_DELAY,
                )
                _log_send(msg)
                data_to_send.remove(msg)

            # get a message
            recv_msg = await sub.get_message(
                timeout_millis=DEFAULT_TIMEOUT_MILLIS,
                retries=DEFAULT_RETRIES,
                retry_delay=DEFAULT_RETRY_DELAY,
            )
            _log_recv_message(recv_msg)

            if not recv_msg:
                logging.info("waiting...")
                continue
            assert recv_msg.data in DATA_LIST

            # message was redelivered, so ack it
            if recv_msg in nacked_msgs:
                logging.info("REDELIVERED!")
                nacked_msgs.remove(recv_msg)
                redelivered_msgs.append(recv_msg)
                await sub.ack_message(
                    recv_msg,
                    retries=DEFAULT_RETRIES,
                    retry_delay=DEFAULT_RETRY_DELAY,
                )
            # otherwise, nack message
            else:
                nacked_msgs.append(recv_msg)
                await sub.reject_message(
                    recv_msg,
                    retries=DEFAULT_RETRIES,
                    retry_delay=DEFAULT_RETRY_DELAY,
                )
                logging.info("NACK!")

        await pub.close()
        await sub.close()

    @pytest.mark.asyncio
    async def test_20(self, queue_name: str, auth_token: str) -> None:
        """Sanity test message generator."""
        pub = await self.broker_client.create_pub_queue(
            "localhost", queue_name, auth_token
        )
        sub = await self.broker_client.create_sub_queue(
            "localhost", queue_name, 1, auth_token
        )

        # send
        for msg in DATA_LIST:
            raw_data = Message.serialize(msg)
            await pub.send_message(
                raw_data,
                retries=DEFAULT_RETRIES,
                retry_delay=DEFAULT_RETRY_DELAY,
            )
            _log_send(msg)

        # receive
        last = 0
        recv_msg: Optional[Message]
        async for i, recv_msg in asl.enumerate(
            sub.message_generator(
                timeout=self.timeout,
                propagate_error=True,
                retries=DEFAULT_RETRIES,
                retry_delay=DEFAULT_RETRY_DELAY,
            )
        ):
            logging.info(i)
            _log_recv_message(recv_msg)
            assert recv_msg
            assert recv_msg.data in DATA_LIST
            last = i
            await sub.ack_message(
                recv_msg,
                retries=DEFAULT_RETRIES,
                retry_delay=DEFAULT_RETRY_DELAY,
            )

        assert last == len(DATA_LIST) - 1

        await pub.close()
        await sub.close()
