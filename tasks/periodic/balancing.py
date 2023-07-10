import asyncio
import time
import datetime
import uuid

import aiohttp

from config import Config
from core.base_task import BaseTask
from clients.enums import PositionSideEnum, RabbitMqQueues


class Balancing(BaseTask):
    __slots__ = 'clients', 'positions', 'total_position', 'disbalance_coin', \
        'disbalance_usd', 'side', 'mq', 'session', 'open_orders', 'app', \
        'chat_id', 'telegram_bot', 'env', 'disbalance_id', 'average_price'  # noqa

    def __init__(self):
        super().__init__()

        self.__set_default()

        for client in self.clients:
            self.clients[client].run_updater()

        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.telegram_bot = Config.TELEGRAM_TOKEN
        self.env = Config.ENV

        time.sleep(15)

    async def run(self, loop) -> None:
        print('START BALANCING')

        async with aiohttp.ClientSession() as session:
            while True:
                await self.setup_mq(loop)

                await self.__close_all_open_orders()
                await self.__get_positions()
                await self.__get_total_positions()
                await self.__balancing_positions(session)

                await self.mq.close()

                self.__set_default()

                time.sleep(Config.TIMEOUT)

    def __set_default(self) -> None:
        self.positions = {}
        self.open_orders = {}
        self.total_position = 0
        self.disbalance_coin = 0
        self.disbalance_usd = 0
        self.disbalance_id = 0
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

    def __get_amount_for_all_clients(self, amount):
        for client in self.clients.values():
            client.fit_amount(amount)

        max_amount = max([client.expect_amount_coin for client in self.clients.values()])

        for client in self.clients.values():
            client.expect_amount_coin = max_amount

    async def __balancing_positions(self, session) -> None:
        tasks = []
        tasks_data = {}

        self.__get_amount_for_all_clients(abs(self.disbalance_coin) / len(self.clients))

        if abs(self.disbalance_usd) > Config.MIN_DISBALANCE:
            self.side = 'sell' if self.disbalance_usd > 0 else 'buy'
            self.disbalance_id = uuid.uuid4()  # noqa

            print('FOUND DISBALANCE')
            for client_name, client in self.clients.items():
                ask_or_bid = 'bids' if self.side == 'LONG' else 'asks'
                price = client.get_orderbook().get(client.symbol, {}).get(ask_or_bid)[0][0]

                if client.get_available_balance(self.side) >= client.expect_amount_coin:
                    tasks.append(client.create_order(side=self.side,
                                                     price=price,
                                                     session=session,
                                                     client_id=f"api_balancing_{str(uuid.uuid4()).replace('-', '')[:20]}"))
                    tasks_data.update({client_name: {'price': price, 'order_place_time': time.time()}})

            await self.__place_and_save_orders(tasks, tasks_data, client.expect_amount_coin)
            await self.save_disbalance()

    async def __place_and_save_orders(self, tasks, tasks_data, amount) -> None:
        for res in await asyncio.gather(*tasks, return_exceptions=True):
            exchange = res['exchange_name']
            order_place_time = res['timestamp'] - tasks_data[exchange]['order_place_time']
            await self.save_orders(self.clients[exchange], tasks_data[exchange]['price'], amount, order_place_time)

    async def save_orders(self, client, expect_price, amount, order_place_time) -> None:
        order_id = uuid.uuid4()
        message = {
            'id': order_id,
            'datetime': datetime.datetime.utcnow(),
            'ts': int(time.time() * 1000),
            'context': 'balancing',
            'parent_id': self.disbalance_id,
            'exchange_order_id': client.LAST_ORDER_ID,
            'type': 'GTT' if client.EXCHANGE_NAME == 'DYDX' else 'GTC',
            'status': 'Processing',
            'exchange': client.EXCHANGE_NAME,
            'side': self.side,
            'symbol': client.symbol,
            'expect_price': client.expect_price,
            'expect_amount_coin': client.expect_amount_coin,
            'expect_amount_usd': client.expect_amount_coin * client.expect_price,
            'expect_fee': client.taker_fee * (amount * expect_price),
            'factual_price': 0,
            'factual_amount_coin': 0,
            'factual_amount_usd': 0,
            'factual_fee': client.taker_fee,
            'order_place_time': order_place_time,
            'env': self.env
        }

        if client.LAST_ORDER_ID == 'default':
            error_message = {
                "chat_id": Config.TELEGRAM_CHAT_ID,
                "msg": f"ALERT NAME: Order Mistake\nOrder Id:{order_id}\nError:{client.error_info}",
                'bot_token': Config.TELEGRAM_TOKEN
            }
            await self.publish_message(connect=self.mq,
                                       message=error_message,
                                       routing_key=RabbitMqQueues.TELEGRAM,
                                       exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.TELEGRAM),
                                       queue_name=RabbitMqQueues.TELEGRAM)
            client.error_info = None

        await self.publish_message(connect=self.mq,
                                   message=message,
                                   routing_key=RabbitMqQueues.ORDERS,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.ORDERS),
                                   queue_name=RabbitMqQueues.ORDERS)

        client.LAST_ORDER_ID = 'default'

    async def save_disbalance(self):
        message = {
            'id': self.disbalance_id,
            'datetime': datetime.datetime.utcnow(),
            'ts': int(time.time() * 1000),
            'coin_name': self.clients['BINANCE'].symbol,
            'position_coin': self.disbalance_coin,
            'position_usd': round(self.disbalance_usd, 1),
            'price': self.average_price,
            'threshold': Config.MIN_DISBALANCE,
            'status': 'Processing'
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
