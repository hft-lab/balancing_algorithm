from tasks.event.check_balance import CheckBalance
from tasks.event.get_orders_results import GetOrdersResults
from tasks.periodic.fundings import Funding
from tasks.periodic.get_all_orders import GetMissedOrders

QUEUES_TASKS = {
    f'logger.event.get_orders_results': GetOrdersResults,
    'logger.periodic.funding': Funding,
    'logger.event.check_balance': CheckBalance,
    'logger.periodic.get_missed_orders': GetMissedOrders
}


class RabbitMqQueues:
    TELEGRAM = 'logger.event.send_to_telegram'
    ORDERS = 'logger.event.insert_orders'
    CHECK_BALANCE = 'logger.event.check_balance'
    DISBALANCE = 'logger.event.insert_disbalances'
    # DEALS_REPORT = 'logger.event.insert_deals_reports'
    # BALANCING_REPORTS = 'logger.event.insert_balancing_reports'
    # PING = 'logger.event.insert_ping_logger'
    # BALANCE_JUMP = 'logger.event.insert_balance_jumps'

    # NEW -----------------------------------------------------------------
    #UPDATE_LAUNCH = 'logger.event.update_bot_launches'
    #ARBITRAGE_POSSIBILITIES = 'logger.event.insert_arbitrage_possibilities'

    # UPDATE_ORDERS = 'logger.event.update_orders'

    #BALANCES = 'logger.event.insert_balances'
    #BALANCE_DETALIZATION = 'logger.event.insert_balance_detalization'

    #FUNDINGS = 'logger.event.insert_funding'
    #SAVE_MISSED_ORDERS = 'logger.event.save_missed_orders'
    #BOT_CONFIG = 'logger.event.insert_bot_config'

    @staticmethod
    def get_exchange_name(routing_key: str):
        routing_list = routing_key.split('.')

        if len(routing_list) > 1 and ('periodic' in routing_key or 'event' in routing_key):
            return routing_list[0] + '.' + routing_list[1]

        raise f'Wrong routing key:{routing_key}'
