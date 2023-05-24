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
        self.__set_default()

        for client in self.clients:
            client.run_updater()
        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.telegram_bot = Config.TELEGRAM_TOKEN
        self.env = Config.ENV
        self.disbalance_id = 0  # noqa

        time.sleep(15)

    async def run(self, event_loop) -> None:
        print('START BALANCING')
        async with aiohttp.ClientSession() as session:
            await self.__setup_mq(event_loop)

            while True:
                await self.__close_all_open_orders()
                await self.__get_positions()
                await self.__check_disbalance()
                await self.__balancing_positions(session)

                self.__set_default()

                time.sleep(Config.TIMEOUT)

    async def __setup_mq(self, event_loop) -> None:
        self.mq = await connect_robust(
            f"amqp://{Config.RABBIT['username']}:{Config.RABBIT['password']}@\
            {Config.RABBIT['host']}:{Config.RABBIT['port']}/",
            loop=event_loop)
        print('SETUP MQ')

    def __set_default(self) -> None:
        self.positions = {}
        self.open_orders = {}
        self.total_position = 0
        self.disbalance_coin = 0  # noqa
        self.disbalance_usd = 0  # noqa
        self.side = 'LONG'

    async def __get_positions(self) -> None:
        prices = []
        for client in self.clients:
            self.positions[client.EXCHANGE_NAME] = client.get_positions().get(client.symbol, {})
            orderbook = client.get_orderbook()[client['symbol']]
            prices.append(orderbook['asks'][0][0] + orderbook['bids'][0][0])
        self.average_price = sum(prices) / len(prices)
        pprint(self.positions)

    async def __check_disbalance(self) -> None: # noqa
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

        self.disbalance_coin = abs(sum([sum(long_coin), sum(short_coin)])) # noqa
        self.disbalance_usd = abs(sum([sum(long_usd), sum(short_usd)])) # noqa
        if self.disbalance_usd > Config.MIN_DISBALANCE:
            self.side = 'sell' if len(long_usd) > len(short_usd) else 'buy'
            self.disbalance_id = uuid.uuid4()  # noqa
            await self.save_disbalance()
            for client in self.clients:
                await self.save_balance_detalization(client, 'pre-balancing')
        print(f'{self.disbalance_coin=}')
        print(f'{self.disbalance_usd=}')
        print(f'{self.side=}')

    async def __close_all_open_orders(self):
        for client in self.clients:
            client.cancel_all_orders()

    async def __balancing_positions(self, session) -> None:
        print('START CHECK AND BALANCING')
        tasks = []
        tasks_data = {}
        amount = self.disbalance_coin / len(self.clients)
        if self.disbalance_usd > Config.MIN_DISBALANCE:
            for client in self.clients:
                order_sent_time = time.time()
                ask_or_bid = 'bids' if self.side == 'LONG' else 'asks'
                price = client.get_orderbook().get(client.symbol, {}).get(ask_or_bid)[0][0]  # noqa
                tasks.append(client.create_order(
                    amount=amount,
                    side=self.side,
                    price=price,
                    session=session))
                tasks_data.update({client.EXCHANGE_NAME: {'price': price, 'order_sent_time': order_sent_time}})

            for res in await asyncio.gather(*tasks, return_exceptions=True):
                exchange = res['exchange_name']
                client = [x for x in self.clients if x.EXCHANGE_NAME == exchange][0]
                order_place_time = res['timestamp'] - tasks_data[exchange]['order_place_time']
                # print(f"Exchange: {client.EXCHANGE_NAME}")
                # print(f"Order response: {res}")
                # if expect_price == res['price']: # QUESTIONING DECISION. NEED DOUBLE EXPERTISE
                await self.save_orders(client,
                                       tasks_data[exchange]['price'],
                                       amount,
                                       order_place_time)

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
            'current_margin': abs(client_position_by_symbol['amount'] * mark_price / client.get_real_balance()),
            'position_coin': client_position_by_symbol['amount'],
            'position_usd': client_position_by_symbol['amount_usd'],
            'entry_price': client_position_by_symbol['entry_price'],
            'mark_price': mark_price
        }
        await self.publish_message(connect=self.mq,
                                   message=message,
                                   routing_key=RabbitMqQueues.BALANCE_DETALIZATION,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.BALANCE_DETALIZATION),
                                   queue_name=RabbitMqQueues.BALANCE_DETALIZATION)

    async def save_disbalance(self):  # noqa
        message = {
            'id': self.disbalance_id,
            'datetime': datetime.datetime.utcnow(),
            'ts': time.time(),
            'coin_name': self.disbalance_coin,
            'position_coin': self.disbalance_usd,
            'position_usd': self.disbalance_usd,
            'price': self.average_price}
        await self.publish_message(connect=self.mq,
                                   message=message,
                                   routing_key=RabbitMqQueues.DISBALANCE,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.DISBALANCE),
                                   queue_name=RabbitMqQueues.DISBALANCE)


if __name__ == '__main__':
    worker = Balancing()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.run(loop))

    # balance_sell_id = uuid.uuid4()
    # await self.save_balance_detalization(balance_sell_id, client_sell)
