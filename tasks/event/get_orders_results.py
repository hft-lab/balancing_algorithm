import asyncio

from core.base_task import BaseTask
from core.enums import RabbitMqQueues


class GetOrdersResults(BaseTask):
    __slots__ = 'app', 'clients', 'orders_results', 'exchange'

    def __init__(self, app):
        super().__init__()
        self.app = app
        self.orders_results = None
        self.exchange = None

    async def run(self, payload):
        await self.__check_all_orders(payload)
        await self.__send_to_save_orders_results()

    async def __check_all_orders(self, payload: list) -> None:
        tasks = []

        self.exchange = payload.pop(-1)

        for order_id in payload:
            tasks.append(self.clients[self.exchange].get_order_by_id(order_id))

        self.orders_results = await asyncio.gather(*tasks, return_exceptions=True)

    async def __send_to_save_orders_results(self):
        await self.publish_message(connect=self.mq,
                                   message={self.exchange: self.orders_results},
                                   routing_key=RabbitMqQueues.UPDATE_ORDERS,
                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.UPDATE_ORDERS),
                                   queue_name=RabbitMqQueues.UPDATE_ORDERS)
