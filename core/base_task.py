from clients.binance import BinanceClient
from clients.dydx import DydxClient
from config import Config


class BaseTask:
    __slots__ = 'db', 'mq', 'clients'

    def __int__(self):
        self.db = None
        self.mq = None
        self.clients = [
            # BitmexClient(Config.BITMEX, Config.LEVERAGE),
            DydxClient(Config.DYDX, Config.LEVERAGE),
            BinanceClient(Config.BINANCE, Config.LEVERAGE),
            # ApolloxClient(Config.APOLLOX, Config.LEVERAGE),
            # OkxClient(Config.OKX, Config.LEVERAGE),
            # KrakenClient(Config.KRAKEN, Config.LEVERAGE)
        ]