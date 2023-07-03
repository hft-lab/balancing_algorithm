import aiohttp

from clients.enums import RabbitMqQueues
from core.base_task import BaseTask


class GetMissedOrders(BaseTask):

    def __init__(self, app):
        super().__init__()
        self.app = app

    async def run(self, payload: dict) -> None:
        orders = []

        async with aiohttp.ClientSession() as session:
            for client in self.clients:
                orders += await client.get_all_orders(payload[client.EXCHANGE_NAME], session)

        for order in orders:
            if 'api_' not in order['client_id']:
                await self.publish_message(connect=self.app['mq'],
                                           message=order,
                                           routing_key=RabbitMqQueues.ORDERS,
                                           exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.ORDERS),
                                           queue_name=RabbitMqQueues.ORDERS
                                           )
