import asyncio
import time
from pprint import pprint

import aiohttp
from aio_pika import connect_robust

from config import Config
from core.base_task import BaseTask
from core.enums import PositionSideEnum


class Balancing(BaseTask):
    __slots__ = 'clients', 'positions', 'total_position', 'disbalance_coin', 'disbalance_usd', 'side', 'mq', 'session', \
                'open_orders'

    def __init__(self):
        self.__set_default()

        for client in self.clients:
            client.run_updater()

        time.sleep(15)

    async def run(self, loop) -> None:
        print('START BALANCING')
        async with aiohttp.ClientSession() as session:
            await self.__setup_mq(loop)

            while True:
                await self.__close_all_open_orders()
                await self.__get_positions()
                await self.__check_disbalance()
                await self.__balancing_positions(session)

                self.__set_default()

                time.sleep(Config.TIMEOUT)

    async def __setup_mq(self, loop) -> None:
        self.mq = await connect_robust(
            f"amqp://{Config.RABBIT['username']}:{Config.RABBIT['password']}@{Config.RABBIT['host']}:{Config.RABBIT['port']}/",
            loop=loop)
        print('SETUP MQ')

    def __set_default(self) -> None:
        self.positions = {}
        self.open_orders = {}
        self.total_position = 0
        self.disbalance_coin = 0
        self.disbalance_usd = 0
        self.side = 'LONG'

    async def __get_positions(self) -> None:
        for client in self.clients:
            self.positions[client.EXCHANGE_NAME] = client.get_positions().get(client.symbol, {})

        pprint(self.positions)

    async def __check_disbalance(self) -> None:
        long_coin = []
        short_coin = []
        long_usd = []
        short_usd = []

        for ecx_name, position in self.positions.items():
            if position and position.get('side') == PositionSideEnum.LONG:
                long_coin.append(position['amount'])
                long_usd.append(position['amount_usd'])
            elif position and position.get('side') == PositionSideEnum.SHORT:
                short_coin.append(position['amount'])
                short_usd.append(position['amount_usd'])

        self.disbalance_coin = abs(sum([sum(long_coin), sum(short_coin)]))
        self.disbalance_usd = abs(sum([sum(long_usd), sum(short_usd)]))
        self.side = 'sell' if len(long_usd) > len(short_usd) else 'buy'

        print(f'{self.disbalance_coin=}')
        print(f'{self.disbalance_usd=}')
        print(f'{self.side=}')

    async def __close_all_open_orders(self):
        for client in self.clients:
            client.cancel_all_orders()


    async def __balancing_positions(self, session) -> None:
        print('START CHECK AND BALANCING')
        tasks = []

        if self.disbalance_usd > Config.MIN_DISBALANCE:
            for client in self.clients:
                price = client.get_orderbook().get(client.symbol, {}).get('bids' if self.side == 'LONG' else 'asks')[0][0]  # noqa
                tasks.append(client.create_order(
                    amount=self.disbalance_coin / len(self.clients),
                    side=self.side,
                    price=price,
                    session=session
                ))

        for res in await asyncio.gather(*tasks, return_exceptions=True):
            print(res)


if __name__ == '__main__':
    worker = Balancing()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.run(loop))
