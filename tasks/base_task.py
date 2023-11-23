import orjson
from aio_pika import Message, ExchangeType, connect_robust

from clients.binance import BinanceClient
from clients.dydx import DydxClient
from clients.apollox import ApolloxClient
from clients.kraken import KrakenClient
from clients.okx import OkxClient

import configparser
import sys
config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")

leverage = float(config['SETTINGS']['LEVERAGE'])


class BaseTask:
    __slots__ = 'mq', 'clients', 'alert_id', 'alert_token'

    def __init__(self):
        self.mq = None
        self.alert_id = config['TELEGRAM']['ALERT_CHAT_ID']
        self.alert_token = config['TELEGRAM']['ALERT_BOT_TOKEN']
        self.clients = {
            # BitmexClient(config['BITMEX'], leverage, self.alert_id, self.alert_token),
            'DYDX': DydxClient(config['DYDX'], leverage, self.alert_id, self.alert_token),
            # 'BINANCE': BinanceClient(config['BINANCE'], leverage, self.alert_id, self.alert_token),
            # 'APOLLOX': ApolloxClient(config['APOLLOX'], leverage, self.alert_id, self.alert_token),
            'OKX': OkxClient(config['OKX'], leverage, self.alert_id, self.alert_token),
            'KRAKEN': KrakenClient(config['KRAKEN'], leverage, self.alert_id, self.alert_token)
        }

    @staticmethod
    async def publish_message(connect, message, routing_key, exchange_name, queue_name):
        channel = await connect.channel()
        exchange = await channel.declare_exchange(exchange_name, type=ExchangeType.DIRECT, durable=True)
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key=routing_key)
        message_body = orjson.dumps(message)
        message = Message(message_body)
        await exchange.publish(message, routing_key=routing_key)
        await channel.close()
        return True

    async def setup_mq(self, event_loop) -> None:
        rabbit = config['RABBIT']
        rabbit_url = f"amqp://{rabbit['USERNAME']}:{rabbit['PASSWORD']}@{rabbit['HOST']}:{rabbit['PORT']}/"
        self.mq = await connect_robust(rabbit_url, loop=event_loop)
        print('SETUP MQ')
