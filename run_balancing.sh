#!/bin/bash

python3 tasks/periodic/balancing.py eth_config.ini &
python3 consumer.py -q logger.event.get_orders_results eth_config.ini &
python3 consumer.py -q logger.event.check_balance eth_config.ini &
python3 producer.py eth_config.ini &
