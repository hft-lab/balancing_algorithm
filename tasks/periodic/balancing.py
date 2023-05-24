import asyncio
import time
import datetime
from pprint import pprint
import uuid

import aiohttp
from aio_pika import connect_robust

from config import Config
from core.base_task import BaseTask
from core.enums import PositionSideEnum, RabbitMqQueues


class Balancing(BaseTask):
    __slots__ = 'clients', 'positions', 'total_position', 'disbalance_coin', \
                'disbalance_usd', 'side', 'mq', 'session', 'open_orders', \
                'chat_id', 'telegram_bot', 'env', 'disbalance_id', 'average_price'  # noqa

    def __init__(self):
        super().__init__()
        self.__set_default()

        for client in self.clients:
            self.clients[client].run_updater()

        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.telegram_bot = Config.TELEGRAM_TOKEN
        self.env = Config.ENV
        self.disbalance_id = 0  # noqa

        time.sleep(15)

    async def run(self, event_loop) -> None:
        print('START BALANCING')
        async with aiohttp.ClientSession() as session:
            await self.setup_mq(event_loop)

            while True:
                await self.__close_all_open_orders()
                await self.__get_positions()
                await self.__get_total_positions()
                await self.__balancing_positions(session)

                self.__set_default()

                time.sleep(Config.TIMEOUT)


    def __set_default(self) -> None:
        self.positions = {}
        self.open_orders = {}
        self.total_position = 0
        self.disbalance_coin = 0  # noqa
        self.disbalance_usd = 0  # noqa
        self.side = 'LONG'

    async def __get_positions(self) -> None:
        prices = []
        for client_name, client in self.clients.items():
            self.positions[client.EXCHANGE_NAME] = client.get_positions().get(client.symbol, {})
            orderbook = client.get_orderbook()[client.symbol]
            prices.append((orderbook['asks'][0][0] + orderbook['bids'][0][0]) / 2)

        self.average_price = sum(prices) / len(prices)
        print(f'{self.positions=}')

    async def __get_total_positions(self) -> None:
        positions = {'long': {'coin': 0, 'usd': 0}, 'short': {'coin': 0, 'usd': 0}}

        for ecx_name, position in self.positions.items():
            if position and position.get('side') == PositionSideEnum.LONG:
                positions['long']['coin'] += position['amount']
                positions['long']['usd'] += position['amount_usd']
            elif position and position.get('side') == PositionSideEnum.SHORT:
                positions['short']['coin'] += position['amount']
                positions['short']['usd'] += position['amount_usd']

        self.disbalance_coin = positions['long']['coin'] + positions['short']['coin']  # noqa
        self.disbalance_usd = positions['long']['usd'] + positions['short']['usd']  # noqa

    async def __close_all_open_orders(self) -> None:
        for _, client in self.clients.items():
            client.cancel_all_orders()

    async def __balancing_positions(self, session) -> None:
        tasks = []
        tasks_data = {}
        amount = abs(self.disbalance_coin) / len(self.clients)

        if abs(self.disbalance_usd) > Config.MIN_DISBALANCE:
            self.side = 'sell' if self.disbalance_usd > 0 else 'buy'
            self.disbalance_id = uuid.uuid4()  # noqa
            await self.save_disbalance()

            print('FOUND DISBALANCE')
            for client_name, client in self.clients.items():
                await self.save_balance_detalization(client, 'pre-balancing')
                ask_or_bid = 'bids' if self.side == 'LONG' else 'asks'
                price = client.get_orderbook().get(client.symbol, {}).get(ask_or_bid)[0][0]  # noqa
                tasks.append(client.create_order(amount=amount, side=self.side, price=price, session=session))
                tasks_data.update({client_name: {'price': price, 'order_place_time':  time.time()}})

            await self.__place_and_save_orders(tasks, tasks_data, amount)

    async def __place_and_save_orders(self, tasks, tasks_data, amount) -> None:
        for res in await asyncio.gather(*tasks, return_exceptions=True):
            exchange = res['exchange_name']
            order_place_time = res['timestamp'] - tasks_data[exchange]['order_place_time']
            await self.save_orders(self.clients[exchange], tasks_data[exchange]['price'], amount, order_place_time)

    async def save_orders(self, client, expect_price, amount, order_place_time) -> None:
        message = {
            'id': uuid.uuid4(),
            'datetime': datetime.datetime.utcnow(),
            'ts': time.time(),
            'context': 'balancing',
            'parent_id': self.disbalance_id,
            'exchange_order_id': client.LAST_ORDER_ID,
            'type': 'GTT' if client.EXCHANGE_NAME == 'DYDX' else 'GTC',
            'status': 'Processing',
            'exchange': client.EXCHANGE_NAME,
            'side': self.side,
            'symbol': client.symbol,
            'expect_price': expect_price,
            'expect_amount_coin': amount,
            'expect_amount_usd': amount * expect_price,
            'expect_fee': client.taker_fee * (amount * expect_price),
            'factual_price': 0,
            'factual_amount_coin': 0,
            'factual_amount_usd': 0,
            'factual_fee': client.taker_fee,
            'order_place_time': order_place_time,
            'env': self.env
        }
        await self.publish_message(connect=self.mq,
                                   message=message,
                                   routing_key=RabbitMqQueues.ORDERS,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.ORDERS),
                                   queue_name=RabbitMqQueues.ORDERS)

    async def save_balance_detalization(self, client, context):  # noqa
        client_position_by_symbol = client.get_positions()[client.symbol]
        mark_price = (client.get_orderbook()[client.symbol]['asks'][0][0] +
                      client.get_orderbook()[client.symbol]['bids'][0][0]) / 2
        message = {
            'id': uuid.uuid4(),
            'datetime': datetime.datetime.utcnow(),
            'ts': time.time(),
            'context': context,
            'parent_id': self.disbalance_id,
            'exchange': client.EXCHANGE_NAME,
            'symbol': client.symbol,
            'max_margin': client.leverage,
            'current_margin': abs(client_position_by_symbol.get('amount', 0) * mark_price / client.get_real_balance()),
            'position_coin': client_position_by_symbol.get('amount', 0),
            'position_usd': client_position_by_symbol.get('amount_usd', 0),
            'entry_price': client_position_by_symbol.get('entry_price', 0),
            'mark_price': mark_price
        }
        await self.publish_message(connect=self.mq,
                                   message=message,
                                   routing_key=RabbitMqQueues.BALANCE_DETALIZATION,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.BALANCE_DETALIZATION),
                                   queue_name=RabbitMqQueues.BALANCE_DETALIZATION)

    async def save_disbalance(self):
        message = {
            'id': self.disbalance_id,
            'datetime': datetime.datetime.utcnow(),
            'ts': time.time(),
            'coin_name': self.clients['BINANCE'].symbol,
            'position_coin': self.disbalance_coin,
            'position_usd': self.disbalance_usd,
            'price': self.average_price
        }

        await self.publish_message(connect=self.mq,
                                   message=message,
                                   routing_key=RabbitMqQueues.DISBALANCE,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.DISBALANCE),
                                   queue_name=RabbitMqQueues.DISBALANCE)


if __name__ == '__main__':
    worker = Balancing()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.run(loop))
