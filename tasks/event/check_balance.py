import asyncio
import time
import uuid
from datetime import datetime

from clients.enums import RabbitMqQueues
from core.base_task import BaseTask


class CheckBalance(BaseTask):

    def __init__(self, app):
        super().__init__()

        self.app = app

        self.chat_id = None
        self.env = None
        self.telegram_bot = None
        self.context = None
        self.parent_id = None
        self.balances = []
        self.positions = []

    async def run(self, payload: dict) -> None:
        await asyncio.sleep(5)
        self.parent_id = payload['parent_id']
        self.context = payload['context']
        self.env = payload['env']
        self.chat_id = payload['chat_id']
        self.telegram_bot = payload['telegram_bot']

        await self.__check_balances()

    async def __check_balances(self) -> None:
        for client in self.clients.values():
            balance_id = uuid.uuid4()
            await self.__save_balance(client, balance_id)

            for symbol in client.get_positions():
                await client.get_orderbook_by_symbol(symbol)
                await self.__save_balance_detalization(symbol, client, balance_id)

    async def __save_balance(self, client, balance_id) -> None:
        sum_amount_usd = sum(
                [x.get('amount_usd', 0) for _, x in client.get_positions().items()])

        message = {
            'id': balance_id,
            'datetime': datetime.utcnow(),
            'ts': time.time(),
            'context': self.context,
            'parent_id': self.parent_id,
            'exchange': client.EXCHANGE_NAME,
            'exchange_balance': client.get_real_balance(),
            'exchange_available_for_buy': client.get_available_balance('buy'),
            'exchange_available_for_sell': client.get_available_balance('sell'),
            'available_for_buy': client.get_real_balance() * 10 - sum(
                [x.get('amount_usd', 0) for _, x in client.get_positions().items()]),
            'available_for_sell': client.get_real_balance() * 10 + sum(
                [x.get('amount_usd', 0) for _, x in client.get_positions().items()]),
            'env': self.env,
            'chat_id': self.chat_id,
            'bot_token': self.telegram_bot,
            'current_margin': abs(sum_amount_usd / client.get_real_balance())
        }

        await self.publish_message(connect=self.app['mq'],
                                   message=message,
                                   routing_key=RabbitMqQueues.BALANCES,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.BALANCES),
                                   queue_name=RabbitMqQueues.BALANCES
                                   )

    async def __save_balance_detalization(self, symbol, client, parent_id):
        client_position_by_symbol = client.get_positions()[symbol]
        mark_price = (client.get_orderbook()[client.symbol]['asks'][0][0] +
                      client.get_orderbook()[client.symbol]['bids'][0][0]) / 2
        position_usd = client_position_by_symbol['amount'] * mark_price
        real_balance = client.get_real_balance()
        message = {
            'id': uuid.uuid4(),
            'datetime': datetime.utcnow(),
            'ts': time.time(),
            'context': self.context,
            'parent_id': parent_id,
            'exchange': client.EXCHANGE_NAME,
            'symbol': symbol,
            'current_margin': abs(client_position_by_symbol['amount_usd'] / real_balance),
            'position_coin': client_position_by_symbol['amount'],
            'position_usd': position_usd,
            'entry_price': client_position_by_symbol['entry_price'],
            'mark_price': mark_price,
            'grand_parent_id': self.parent_id,
            'available_for_buy': real_balance * client.leverage - position_usd,
            'available_for_sell': real_balance * client.leverage + position_usd
        }

        await self.publish_message(connect=self.app['mq'],
                                   message=message,
                                   routing_key=RabbitMqQueues.BALANCE_DETALIZATION,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.BALANCE_DETALIZATION),
                                   queue_name=RabbitMqQueues.BALANCE_DETALIZATION
                                   )
