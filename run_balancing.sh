#!/bin/bash

python3 balancing.py eth_config.ini &
python3 consumer.py eth_config.ini &
python3 producer.py eth_config.ini &
