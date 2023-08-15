class PeriodicTasks:
    SECOND = 1
    MINUTE = 60
    TEN_MINUTES = MINUTE * 10
    HOUR = MINUTE * 60
    DAY = HOUR * 24

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