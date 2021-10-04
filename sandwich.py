import argparse
import asyncio
import brotli
import traceback
import os

from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN

from stan.aio.client import Msg

import grpc

import protobuf.events_pb2_grpc as events_pb2_grpc
import protobuf.events_pb2 as events_pb2
import protobuf.discord_pb2 as discord_pb2


class SandwichClient:
    def __init__(
        self,
        cluster_id: str,
        client_id: str,
        subject: str,
        loop: asyncio.AbstractEventLoop = None
    ):
        self.loop = asyncio.get_event_loop() if loop is None else loop

        self._cluster_id: str = cluster_id
        self._client_id: str = client_id
        self._subject: str = subject

        self._channel = grpc.aio.insecure_channel("127.0.0.1:15000")
        self.stub = events_pb2_grpc.SandwichStub(self._channel)

    async def grpc_test(self):
        response: events_pb2.GuildsResponse = await self.stub.FetchGuild(events_pb2.FetchGuildRequest(query="A"))
        print(response.guildIDs, response.guilds, response.baseResponse.ok)

        os._exit(0)

    async def _on_exception(self, err: Exception):
        print(traceback.format_exc())
        os._exit(0)

    async def _on_message(self, msg: Msg):
        msgData = brotli.decompress(msg.data)
        print(msgData)

        await self.grpc_test()
        os._exit(0)

    async def run(self):
        self.nc = NATS()
        self.sc = STAN()

        await self.nc.connect(io_loop=self.loop)
        await self.sc.connect(
            cluster_id=self._cluster_id,
            client_id=self._client_id,
            nats=self.nc
        )

        self.sub = await self.sc.subscribe(
            subject=self._subject,
            cb=self._on_message,
            error_cb=self._on_exception
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Handles events received from Sandwich through STAN")

    parser.add_argument("cluster_id", type=str, help="NATs Cluster ID")
    parser.add_argument("client_id", type=str, help="NATs Client ID")
    parser.add_argument("subject", type=str, help="NATs Subject Name")

    args = parser.parse_args()

    loop = asyncio.get_event_loop()

    sandwich = SandwichClient(
        cluster_id=args.cluster_id,
        client_id=args.client_id,
        subject=args.subject,
        loop=loop
    )

    loop.run_until_complete(sandwich.run())
    loop.run_forever()
