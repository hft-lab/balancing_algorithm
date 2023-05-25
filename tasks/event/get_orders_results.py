import asyncio

import aiohttp

from core.base_task import BaseTask
from core.enums import RabbitMqQueues


class GetOrdersResults(BaseTask):
    __slots__ = 'app', 'clients', 'order_result'

    def __init__(self, app):
        super().__init__()
        self.app = app
        self.order_result = {}

    async def run(self, payload) -> None:
        print('START')
        await self.__check_all_orders(payload)
        await self.__send_to_save_orders_results()


    async def __check_all_orders(self, payload: dict) -> None:
        try:
            async with aiohttp.ClientSession() as session:
                self.order_result = await self.clients[payload['exchange']].get_order_by_id(payload['order_ids'], session)
                print(f'{self.order_result=}')

        except aiohttp.ServerDisconnectedError:
            await self.__check_all_orders(payload)

    async def __send_to_save_orders_results(self):
        if self.order_result:

            await self.publish_message(connect=self.app['mq'],
                                       message=self.order_result,
                                       routing_key=RabbitMqQueues.UPDATE_ORDERS,
                                       exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.UPDATE_ORDERS),
                                       queue_name=RabbitMqQueues.UPDATE_ORDERS)
