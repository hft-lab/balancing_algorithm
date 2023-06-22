import argparse
import asyncio
import logging
import traceback
from logging.config import dictConfig

import orjson
from aio_pika import connect_robust
from aiohttp.web import Application

from config import Config
from tasks.event.check_balance import CheckBalance
from tasks.event.get_orders_results import GetOrdersResults
from tasks.periodic.fundings import Funding


dictConfig(Config.LOGGING)
logger = logging.getLogger(__name__)

TASKS = {
    f'logger.event_{Config.GLOBAL_SYMBOL}.get_orders_results': GetOrdersResults,
    'logger.periodic.funding': Funding,
    'logger.event.check_balance': CheckBalance
}


class Consumer:
    """
    Producer get periodic and events tasks from RabbitMQ
    """

    def __init__(self, loop, queue=None):
        self.app = Application()
        self.loop = loop
        self.queue = queue
        self.rabbit_url = f"amqp://{Config.RABBIT['username']}:{Config.RABBIT['password']}@{Config.RABBIT['host']}:{Config.RABBIT['port']}/"  # noqa
        self.periodic_tasks = []

    async def run(self) -> None:
        """
        Init setup db connection and star tasks from queue
        :return: None
        """
        await self.setup_mq()

        logger.info(f"Queue: {self.queue}")
        logger.info(f"Exist queue: {self.queue in TASKS}")

        if self.queue and self.queue in TASKS:
            logger.info("Single work option")
            self.periodic_tasks.append(self.loop.create_task(self._consume(self.app['mq'], self.queue)))


    async def setup_mq(self):
        self.app['mq'] = await connect_robust(self.rabbit_url, loop=self.loop)

    async def _consume(self, connection, queue_name) -> None:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.consume(self.on_message)

    async def on_message(self, message) -> None:
        logger.info(f"\n\nReceived message {message.routing_key}")
        try:
            if 'logger.periodic' in message.routing_key:
                await message.ack()

            task = TASKS.get(message.routing_key)(self.app)
            await task.run(orjson.loads(message.body))
            logger.info(f"Success task {message.routing_key}")
            if 'logger.event' in message.routing_key:
                await message.ack()
        except Exception as e:
            logger.info(f"Error {e} while serving task {message.routing_key}")
            traceback.print_exc()
            await message.ack()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', nargs='?', const=True, dest='queue', default='logger.event.check_balance')
    args = parser.parse_args()

    loop = asyncio.get_event_loop()

    worker = Consumer(loop, queue=args.queue.strip())
    loop.run_until_complete(worker.run())

    try:
        loop.run_forever()
    finally:
        loop.close()