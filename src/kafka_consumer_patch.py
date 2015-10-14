import logging
from kafka.common import KafkaConfigurationError
from kafka.consumer.kafka import DEFAULT_CONSUMER_CONFIG, logger, KafkaConsumer


def __configure_patched(self, **configs):
    logging.info('Monkey patched Kafka Consumer')

    configs = self._deprecate_configs(**configs)
    self._config = {}
    for key in DEFAULT_CONSUMER_CONFIG:
        self._config[key] = configs.pop(key, DEFAULT_CONSUMER_CONFIG[key])

    if self._config['auto_commit_enable']:
        if not self._config['group_id']:
            raise KafkaConfigurationError(
                'KafkaConsumer configured to auto-commit '
                'without required consumer group (group_id)'
            )

    # Check auto-commit configuration
    if self._config['auto_commit_enable']:
        logger.info("Configuring consumer to auto-commit offsets")
        self._reset_auto_commit()

    if not configs['kafka_client']:
        raise KafkaConfigurationError(
            'bootstrap_servers required to configure KafkaConsumer'
        )

    self._client = configs['kafka_client']


def monkey_patch_kafka_consumer():
    KafkaConsumer.configure = __configure_patched
