import orjson
from aio_pika import Message, ExchangeType, connect_robust

from clients.binance import BinanceClient
from clients.dydx import DydxClient
from clients.apollox import ApolloxClient
from clients.kraken import KrakenClient

import configparser
import sys
config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")

class BaseTask:
    __slots__ = 'mq', 'clients'

    def __init__(self):
        self.mq = None

        self.clients = {
            # BitmexClient(config['BITMEX'], float(config['SETTINGS']['LEVERAGE'])),
            'DYDX': DydxClient(config['DYDX'], float(config['SETTINGS']['LEVERAGE'])),
            'BINANCE': BinanceClient(config['BINANCE'], float(config['SETTINGS']['LEVERAGE'])),
            'APOLLOX': ApolloxClient(config['APOLLOX'], float(config['SETTINGS']['LEVERAGE'])),
            # OkxClient(config['SETTINGS']OKX, float(config['SETTINGS']['LEVERAGE'])),
            'KRAKEN': KrakenClient(config['KRAKEN'], float(config['SETTINGS']['LEVERAGE']))
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
