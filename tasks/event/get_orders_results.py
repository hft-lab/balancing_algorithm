import traceback

import aiohttp

from tasks.all_tasks import RabbitMqQueues
from tasks.base_task import BaseTask
from core.wrappers import try_exc_regular, try_exc_async


class GetOrdersResults(BaseTask):
    __slots__ = 'app', 'clients', 'order_result'

    def __init__(self, app):
        super().__init__()
        self.app = app

    @try_exc_async
    async def run(self, payload) -> None:
        for data in payload:
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


