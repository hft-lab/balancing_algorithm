from clients.binance import BinanceClient
from clients.dydx import DydxClient
from config import Config
from aio_pika import Message, ExchangeType, connect_robust
import traceback
import orjson


class BaseTask:
    __slots__ = 'mq', 'clients'

    def __init__(self):
        self.mq = None
        self.clients = {
            # BitmexClient(Config.BITMEX, Config.LEVERAGE),
            'DYDX': DydxClient(Config.DYDX, Config.LEVERAGE),
            'BINANCE': BinanceClient(Config.BINANCE, Config.LEVERAGE),
            # ApolloxClient(Config.APOLLOX, Config.LEVERAGE),
            # OkxClient(Config.OKX, Config.LEVERAGE),
            # KrakenClient(Config.KRAKEN, Config.LEVERAGE)
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
        self.mq = await connect_robust(
            f"amqp://{Config.RABBIT['username']}:{Config.RABBIT['password']}@{Config.RABBIT['host']}:"
            f"{Config.RABBIT['port']}/", loop=event_loop
        )
        print('SETUP MQ')
