from os import getenv

from dotenv import load_dotenv

load_dotenv()

SECOND = 1
MINUTE = 60
TEN_MINUTES = MINUTE * 10
HOUR = MINUTE * 60
DAY = HOUR * 24

GLOBAL_SYMBOL = getenv('GLOBAL_SYMBOL')


class Config:
    MIN_DISBALANCE = float(getenv('MIN_DISBALANCE'))
    LEVERAGE = float(getenv('LEVERAGE'))
    TIMEOUT = float(getenv('TIMEOUT', 180))
    ENV = getenv('ENV')
    TELEGRAM_CHAT_ID = getenv('TELEGRAM_CHAT_ID')
    TELEGRAM_TOKEN = getenv('TELEGRAM_TOKEN')
    GLOBAL_SYMBOL = getenv("GLOBAL_SYMBOL")

    RABBIT = {
        "host": getenv("RABBIT_HOST"),
        "port": getenv("RABBIT_PORT", 5672),
        "username": getenv("RABBIT_USERNAME"),
        "password": getenv("RABBIT_PASSWORD")
    }

    BITMEX = {
        "api_key": getenv("BITMEX_API_KEY"),
        "api_secret": getenv("BITMEX_API_SECRET"),
        "symbol": getenv("BITMEX_SYMBOL"),
        "bitmex_shift": int(getenv("BITMEX_SHIFT"))
    }

    DYDX = {
        "eth_address": getenv("DYDX_ETH_ADDRESS"),
        "privateKey": getenv("DYDX_PRIVATE_KEY"),
        "publicKeyYCoordinate": getenv("DYDX_PUBLIC_KEY_Y_COORDINATE"),
        "publicKey": getenv("DYDX_PUBLIC_KEY"),
        "eth_private_key": getenv("DYDX_ETH_PRIVATE_KEY"),
        "infura_key": getenv("DYDX_INFURA_KEY"),
        "key": getenv("DYDX_KEY"),
        "secret": getenv("DYDX_SECRET"),
        "passphrase": getenv("DYDX_PASSPHRASE"),
        "symbol": getenv("DYDX_SYMBOL"),
        "dydx_shift": int(getenv("DYDX_SHIFT"))
    }

    BINANCE = {
        "api_key": getenv("BINANCE_API_KEY"),
        "secret_key": getenv("BINANCE_SECRET_API"),
        "symbol": getenv("BINANCE_SYMBOL"),
        "binance_shift": int(getenv("BINANCE_SHIFT"))
    }

    OKX = {
        "public_key": getenv("OKEX_PUBLIC_KEY"),
        "secret_key": getenv("OKEX_SECRET_KEY"),
        "passphrase": getenv("OKEX_PASSPHRASE"),
        "symbol": getenv("OKEX_SYMBOL"),
        "okex_shift": int(getenv("OKEX_SHIFT"))
    }

    KRAKEN = {
        "api_key": getenv("KRAKEN_API_KEY"),
        "secret_key": getenv("KRAKEN_SECRET_API"),
        "symbol": getenv("KRAKEN_SYMBOL"),
        "apollox_shift": int(getenv("KRAKEN_SHIFT"))
    }

    APOLLOX = {
        "api_key": getenv("APOLLOX_API_KEY"),
        "secret_key": getenv("APOLLOX_SECRET_API"),
        "symbol": getenv("APOLLOX_SYMBOL"),
        "apollox_shift": int(getenv("APOLLOX_SHIFT"))
    }

    PERIODIC_TASKS = [
        {
            'exchange': f'logger.periodic',
            'queue': f'logger.periodic.funding',
            'routing_key': f'logger.periodic.funding',
            'interval': HOUR,
            'delay': SECOND * 10,
            'payload': {}
        },
        {
            'exchange': 'logger.periodic',
            'queue': 'logger.periodic.get_missed_orders',
            'routing_key': 'logger.periodic.get_missed_orders',
            'interval': DAY,
            'delay': SECOND * 10,
            'payload': {
                'DYDX': 'ETH-USD',
                'BINANCE': 'ETHUSDT',
                'APOLLOX': 'ETHUSDT',
                'KRAKEN': 'PF_ETHUSD'
            }
        },
        {
            'exchange': f'logger.periodic',
            'queue': f'logger.periodic.get_missed_orders',
            'routing_key': f'logger.periodic.get_missed_orders',
            'interval': DAY,
            'delay': SECOND * 10,
            'payload': {
                'DYDX': 'BTCUSD',
                'BINANCE': 'BTCUSDT',
                'APOLLOX': 'BTCUSDT',
                'KRAKEN': 'PF_XBTUSD'
            }
        }
    ]

    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '[%(asctime)s][%(threadName)s] %(funcName)s: %(message)s'
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'simple',
                'stream': 'ext://sys.stdout'
            },
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'level': 'DEBUG',
                'propagate': False
            },
        }
    }
