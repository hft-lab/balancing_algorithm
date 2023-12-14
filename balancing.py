import asyncio
import time
from datetime import datetime
import uuid
import aiohttp
from tasks.all_tasks import RabbitMqQueues
from tasks.base_task import BaseTask
from clients.enums import PositionSideEnum
import configparser
import sys
from core.wrappers import try_exc_regular, try_exc_async
import random

config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")


class Balancing(BaseTask):
    __slots__ = 'clients', 'positions', 'total_position', 'disbalances', \
                'side', 'mq', 'session', 'open_orders', 'app', \
                'chat_id', 'chat_token', 'env', 'disbalance_id', 'average_price', \
                'orderbooks' # noqa

    def __init__(self):
        super().__init__()
        self.__set_default()
        self.orderbooks = {}
        self.env = config['SETTINGS']['ENV']
        time.sleep(15)

    @try_exc_async
    async def run(self, loop) -> None:
        print('START BALANCING')
        async with aiohttp.ClientSession() as session:
            while True:
                await self.setup_mq(loop)
                for exchange, client in self.clients.items():
                    client.get_position()
                await self.__close_all_open_orders()
                await self.update_balances()
                await self.__get_positions()
                await self.__get_total_positions()
                await self.send_positions_message(self.create_positions_message())
                await self.__balancing_positions(session)
                await self.mq.close()
                print(f"MQ CLOSED")

                self.__set_default()

                time.sleep(int(config['SETTINGS']['TIMEOUT']))

    @try_exc_regular
    def __set_default(self) -> None:
        self.positions = {}
        self.open_orders = {}
        self.total_position = 0
        self.disbalances = {}
        self.disbalance_id = uuid.uuid4()

    @try_exc_async
    async def update_balances(self):
        for client_name, client in self.clients.items():
            client.get_real_balance()

    @try_exc_async
    async def __get_positions(self) -> None:
        for client_name, client in self.clients.items():
            for symbol, position in client.get_positions().items():
                coin = self.get_coin(symbol)
                # orderbook = self.orderbooks[client_name][symbol]
                position.update({'symbol': symbol})
                if not self.positions.get(coin):
                    self.positions.update({coin: {client_name: position}})
                else:
                    self.positions[coin].update({client_name: position})

    @staticmethod
    @try_exc_regular
    def get_coin(symbol):
        coin = ''
        if '_' in symbol:
            coin = symbol.split('_')[1].upper().split('USD')[0]
        elif '-' in symbol:
            coin = symbol.split('-')[0]
        elif 'USDT' in symbol:
            coin = symbol.split('USD')[0]
        return coin

    @try_exc_async
    async def get_mark_price(self, coin):
        clients_list = list(self.clients.values())
        random_client = clients_list[random.randint(0, len(clients_list) - 1)]
        ob = await random_client.get_orderbook_by_symbol(random_client.markets[coin])
        mark_price = (ob['asks'][0][0] + ob['bids'][0][0]) / 2
        return mark_price

    @try_exc_async
    async def __get_total_positions(self) -> None:
        positions = {}
        for coin, exchanges in self.positions.items():
            mark_price = self.get_mark_price(coin)
            positions.update({coin: {'long': {'coin': 0, 'usd': 0}, 'short': {'coin': 0, 'usd': 0}}})
            self.disbalances.update({coin: {}})
            for exchange, position in exchanges.items():
                pos_usd = position['amount'] * mark_price
                if position and position.get('side') == PositionSideEnum.LONG:
                    positions[coin]['long']['coin'] += position['amount']
                    positions[coin]['long']['usd'] += pos_usd
                elif position and position.get('side') == PositionSideEnum.SHORT:
                    positions[coin]['short']['coin'] += position['amount']
                    positions[coin]['short']['usd'] += pos_usd
            disb_coin = positions[coin]['long']['coin'] + positions[coin]['short']['coin']
            disb_usd = positions[coin]['long']['usd'] + positions[coin]['short']['usd']
            self.disbalances[coin].update({'coin': disb_coin})  # noqa
            self.disbalances[coin].update({'usd': disb_usd})
        if len(list(self.disbalances)) > 2:
            message = f"ALERT: MORE THAN ONE DISBALANCE. POSITIONS: {self.positions}"
            await self.publish_message(connect=self.mq,
                                       message=message,
                                       routing_key=RabbitMqQueues.TELEGRAM,
                                       exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.TELEGRAM),
                                       queue_name=RabbitMqQueues.TELEGRAM)

    @try_exc_regular
    def create_positions_message(self):
        refactored_positions = {}
        for coin, exchanges in self.positions.items():
            for exchange, position in exchanges.items():
                if refactored_positions.get(exchange):
                    refactored_positions[exchange]['total_position'] += int(round(position['amount_usd']))
                    refactored_positions[exchange]['abs_position'] += abs(int(round(position['amount_usd'])))
                    refactored_positions[exchange]['num_positions'] += 1
                else:
                    refactored_positions.update({exchange:
                                                     {'total_position': int(round(position['amount_usd'])),
                                                      'abs_position': abs(int(round(position['amount_usd']))),
                                                      'num_positions': 1}})
        return self.compose_message(refactored_positions)

    @try_exc_regular
    def compose_message(self, refactored_positions):
        tot_pos = 0
        abs_pos = 0
        message = "    POSITIONS:"
        for exchange, data in refactored_positions.items():
            tot_pos += data['total_position']
            abs_pos += data['abs_position']
            message += f"\n  {exchange}"
            message += f"\nTOT POS, USD: {data['total_position']}"
            message += f"\nABS POS, USD: {data['abs_position']}"
            message += f"\nPOSITIONS, NUM: {data['num_positions']}"
        total_balance = 0
        message += f"\n    BALANCES:"
        for exc_name, client in self.clients.items():
            exc_bal = client.get_balance()
            message += f"\n{exc_name}, USD: {int(round(exc_bal, 0))}"
            total_balance += exc_bal
        message += f"\n    TOTAL:"
        message += f"\nBALANCE, USD: {int(round(total_balance, 0))}"
        message += f"\nTOT POSITION, USD: {tot_pos}"
        message += f"\nABS POSITION, USD: {abs_pos}"
        message += f"\nEFFECTIVE LEVERAGE: {round(abs_pos / total_balance, 2)}"
        for coin, disbalance in self.disbalances.items():
            if abs(disbalance['usd']) > int(config['SETTINGS']['MIN_DISBALANCE']):
                message += f"\nDISB, {coin}: {round(disbalance['coin'], 4)}"
                message += f" (USD: {int(round(disbalance['usd'], 0))})"
        return message

    @try_exc_async
    async def send_positions_message(self, message):
        send_message = {
            "chat_id": self.chat_id,
            "msg": message,
            'bot_token': self.chat_token
        }
        await self.publish_message(connect=self.mq,
                                   message=send_message,
                                   routing_key=RabbitMqQueues.TELEGRAM,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.TELEGRAM),
                                   queue_name=RabbitMqQueues.TELEGRAM)

    @try_exc_async
    async def __close_all_open_orders(self) -> None:
        for _, client in self.clients.items():
            client.cancel_all_orders()

    @try_exc_async
    async def __get_amount_for_all_clients(self, amount, exchanges, coin, side):
        for exchange in exchanges:
            symbol = self.positions[coin][exchange]['symbol']
            step_size = self.clients[exchange].instruments[symbol]['step_size']
            size = round(amount / step_size) * step_size
            if size < self.clients[exchange].instruments[symbol]['min_size']:
                self.clients[exchange].amount = 0
                continue
            self.clients[exchange].amount = size
            position = self.positions[coin][exchange]
            ob = await self.clients[exchange].get_orderbook_by_symbol(symbol)
            price = ob['asks'][3][0] if side == 'buy' else ob['bids'][3][0]
            self.clients[exchange].fit_sizes(price, position['symbol'])

        # max_amount = max([client.expect_amount_coin for client in self.clients.values()])
        #
        # for client in self.clients.values():
        #     client.expect_amount_coin = max_amount

    @try_exc_async
    async def __balancing_positions(self, session) -> None:
        for coin, disbalance in self.disbalances.items():
            tasks = []
            tasks_data = {}
            exchanges = list(self.positions[coin].keys())
            if abs(disbalance['usd']) > int(config['SETTINGS']['MIN_DISBALANCE']):
                side = 'sell' if disbalance['usd'] > 0 else 'buy'
                self.disbalance_id = uuid.uuid4()  # noqa
            else:
                continue
            await self.__get_amount_for_all_clients(abs(disbalance['coin']) / len(exchanges), exchanges, coin, side)
            for exchange in exchanges:
                if not self.clients[exchange].amount:
                    exchanges.remove(exchange)
                    continue
                symbol = self.positions[coin][exchange]['symbol']
                client_id = f"api_balancing_{str(uuid.uuid4()).replace('-', '')[:20]}"
                tasks.append(self.clients[exchange].create_order(symbol=symbol,
                                                                 side=side,
                                                                 session=session,
                                                                 client_id=client_id))
                tasks_data.update({exchange: {'order_place_time': int(time.time() * 1000)}})

            await self.place_and_save_orders(tasks, tasks_data, coin, side)
            await self.save_disbalance(coin, self.clients[exchanges[0]])
            await self.save_balance()
            await self.send_balancing_message(exchanges, coin, side)

    @try_exc_async
    async def send_balancing_message(self, exchanges, coin, side):
        message = 'BALANCING PROCEED:\n'
        message += f"COIN: {coin}\n"
        message += f"SIDE: {side}\n"
        message += f"ORDER SIZE PER EXCHANGE, {coin}: {self.clients[exchanges[0]].amount}\n"
        message += f"EXCHANGES: {'|'.join(exchanges)}\n"
        send_message = {
            "chat_id": self.chat_id,
            "msg": message,
            'bot_token': self.chat_token
        }
        await self.publish_message(connect=self.mq,
                                   message=send_message,
                                   routing_key=RabbitMqQueues.TELEGRAM,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.TELEGRAM),
                                   queue_name=RabbitMqQueues.TELEGRAM)

    @try_exc_async
    async def place_and_save_orders(self, tasks, tasks_data, coin, side) -> None:
        for res in await asyncio.gather(*tasks, return_exceptions=True):
            exchange = res['exchange_name']
            order_place_time = res['timestamp'] - tasks_data[exchange]['order_place_time']
            await self.save_orders(self.clients[exchange],
                                   self.clients[exchange].price,
                                   self.clients[exchange].amount,
                                   order_place_time,
                                   coin, side)

    @try_exc_async
    async def save_balance(self) -> None:
        message = {
            'parent_id': self.disbalance_id,
            'context': 'post-balancing',
            'env': self.env,
            'chat_id': self.chat_id,
            'telegram_bot': self.chat_token,
        }

        await self.publish_message(connect=self.mq,
                                   message=message,
                                   routing_key=RabbitMqQueues.CHECK_BALANCE,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.CHECK_BALANCE),
                                   queue_name=RabbitMqQueues.CHECK_BALANCE)

    @try_exc_async
    async def save_orders(self, client, expect_price, amount, order_place_time, coin, side) -> None:
        order_id = uuid.uuid4()
        message = {
            'id': order_id,
            'datetime': datetime.utcnow(),
            'ts': int(time.time() * 1000),
            'context': 'balancing',
            'parent_id': self.disbalance_id,
            'exchange_order_id': client.LAST_ORDER_ID,
            'type': 'GTT' if client.EXCHANGE_NAME == 'DYDX' else 'GTC',
            'status': 'Processing',
            'exchange': client.EXCHANGE_NAME,
            'side': side,
            'symbol': self.positions[coin][client.EXCHANGE_NAME]['symbol'],
            'expect_price': client.price,
            'expect_amount_coin': client.amount,
            'expect_amount_usd': client.amount * client.price,
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
                "chat_id": self.chat_id,
                "msg": f"ALERT NAME: Order Mistake\nCOIN: {coin}\nCONTEXT: BOT\nENV: {self.env}\nEXCHANGE: "
                       f"{client.EXCHANGE_NAME}\nOrder Id:{order_id}\nError:{client.error_info}",
                'bot_token': self.chat_token
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

    @try_exc_async
    async def save_disbalance(self, coin, client):
        message = {
            'id': self.disbalance_id,
            'datetime': datetime.utcnow(),
            'ts': int(datetime.utcnow().timestamp() * 1000),
            'coin_name': coin,
            'position_coin': self.disbalances[coin]['coin'],
            'position_usd': round(self.disbalances[coin]['usd'], 1),
            'price': client.price,
            'threshold': float(config['SETTINGS']['MIN_DISBALANCE']),
            'status': 'Processing'
        }

        await self.publish_message(connect=self.mq,
                                   message=message,
                                   routing_key=RabbitMqQueues.DISBALANCE,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.DISBALANCE),
                                   queue_name=RabbitMqQueues.DISBALANCE)


if __name__ == '__main__':
    worker = Balancing()
    loop = asyncio.new_event_loop()
    loop.run_until_complete(worker.run(loop))
