import argparse
import asyncio
import logging
import traceback
from logging.config import dictConfig
import time
import random

import orjson
from aio_pika import connect_robust
from aiohttp.web import Application

from tasks.event.check_balance import CheckBalance
from tasks.event.get_orders_results import GetOrdersResults
from tasks.periodic.fundings import Funding
from tasks.periodic.get_all_orders import GetMissedOrders

import configparser
import sys
config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")

dictConfig({'version': 1, 'disable_existing_loggers': False, 'formatters': {
                'simple': {'format': '[%(asctime)s][%(threadName)s] %(funcName)s: %(message)s'}},
            'handlers': {'console': {'class': 'logging.StreamHandler', 'level': 'DEBUG', 'formatter': 'simple',
                'stream': 'ext://sys.stdout'}},
            'loggers': {'': {'handlers': ['console'], 'level': 'DEBUG', 'propagate': False}}})
logger = logging.getLogger(__name__)

TASKS = {
    f'logger.event.get_orders_results': GetOrdersResults,
    'logger.periodic.funding': Funding,
    'logger.event.check_balance': CheckBalance,
    'logger.periodic.get_missed_orders': GetMissedOrders
}


class Consumer:
    """
    Producer get periodic and events tasks from RabbitMQ
    """

    def __init__(self, loop, queue=None):
        time.sleep(random.randint(1, 10))
        self.app = Application()
        self.loop = loop
        self.queue = queue
        rabbit = config['RABBIT']
        self.rabbit_url = f"amqp://{rabbit['USERNAME']}:{rabbit['PASSWORD']}@{rabbit['HOST']}:{rabbit['PORT']}/"  # noqa
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
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-q', nargs='?', const=True, dest='queue', default='logger.periodic.get_missed_orders')
    # args = parser.parse_args()
    import threading
    import multiprocessing

    def async_process(queue):
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)

        worker = Consumer(loop, queue=queue)
        loop.run_until_complete(worker.run())
        try:
            loop.run_forever()
        except Exception:
            traceback.print_exc()
        finally:
            loop.close()


    if __name__ == '__main__':
        queues = config['SETTINGS']['QUEUES'].split(',')

        processes = []
        for queue in queues:
            process = multiprocessing.Process(target=async_process, args=(queue,))
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

    # def main_loop(queue):
    #     try:
    #         loop = asyncio.get_event_loop()
    #
    #         worker = Consumer(loop, queue=queue)
    #         loop.run_until_complete(worker.run())
    #
    #         # You can add other tasks or logic here
    #
    #     except Exception:
    #         traceback.print_exc()


    # Run the main loop multiple times
    # for queue in queues:
    #     main_loop(queue)
    # threads = []

    # #

    # # #
    # # #
    # # # asyncio.run(main())
    # # for queue in queues:
    # #     asyncio.run(process_queue(queue))
    # self.loop_1 = asyncio.new_event_loop()
    # t1 = threading.Thread(target=self.run_await_in_thread, args=[self.__start, self.loop_1])
    # t1.start()
    # t1.join(0-9)
    #
    # loop = asyncio.get_event_loop()
    #
    # worker = Consumer(loop, queue=queues[0])
    # loop.run_until_complete(worker.run())
    #
    # try:
    #     loop.run_forever()
    # except Exception:
    #     traceback.print_exc()
    # finally:
    #     loop.close()

