import traceback

import aiohttp

from core.base_task import BaseTask
from clients.enums import RabbitMqQueues


class GetOrdersResults(BaseTask):
    __slots__ = 'app', 'clients', 'order_result'

    def __init__(self, app):
        super().__init__()
        self.app = app

    async def run(self, payload) -> None:
        for data in payload:
            try:
                async with aiohttp.ClientSession() as session:
                    if res := await self.clients[data['exchange']].get_order_by_id(data['symbol'],
                                                                                   data['order_ids'],
                                                                                   session):
                        print(f'{res=}')
                        await self.publish_message(connect=self.app['mq'],
                                                   message=res,
                                                   routing_key=RabbitMqQueues.UPDATE_ORDERS,
                                                   exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.UPDATE_ORDERS),
                                                   queue_name=RabbitMqQueues.UPDATE_ORDERS)

            except (aiohttp.ServerDisconnectedError, ConnectionResetError):
               traceback.print_exc()

