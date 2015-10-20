import logging
import os
from zookeeper import ZookeeperClient

class LocalSubscriptionHolder(object):
    # FIXME: not thread safe!
    def __init__(self):
        self.store = []

    def get_currently_subscribed_topics(self):
        topics_with_duplicates = map(lambda subscriber: subscriber['topic'], self.store)
        unique_topics = set(topics_with_duplicates)
        return unique_topics

    def add(self, id, topic, callback):
        subscription = create_subscription_as_dict(topic, callback)
        subscription['id'] = id
        self.store.append(subscription)
        return subscription

    def remove(self, id):
        self.store = list([subscription for subscription in self.store if subscription['id'] != id])

    def get(self, topic):
        return [subscription for subscription in self.store if subscription['topic'] == topic]

    def as_dict(self):
        return self.store


class ZookeeperSubscriptionHolder(object):

    def __init__(self, hosts):
        self.zk = ZookeeperClient(hosts)
        self.entity = 'subscriptions'

    def get_currently_subscribed_topics(self):
        subscribers = self.as_dict()
        topics_with_duplicates = map(lambda subscriber: subscriber['topic'], subscribers)
        unique_topics = set(topics_with_duplicates)
        return unique_topics

    def add(self, id, topic, callback):
        subscription = create_subscription_as_dict(topic, callback)
        self.zk.put(self.entity, id, subscription)
        subscription['id'] = id
        return subscription

    def remove(self, id):
        self.zk.delete(self.entity, id)

    def get(self, topic):
        return [subscription for subscription in self.as_dict() if subscription['topic'] == topic]

    def __transform_from_zk(self, zk_subscription):
        id, subscription = zk_subscription
        subscription['id'] = id
        return subscription

    def as_dict(self):
        zk_subscriptions = self.zk.list(self.entity)
        return list(map(self.__transform_from_zk, zk_subscriptions))

def build_subscription_holder(mode='local'):
    if mode == 'local':
        return LocalSubscriptionHolder()
    elif mode == 'zookeeper':
        return ZookeeperSubscriptionHolder(os.environ.get('ZK_BROKERS', 'localhost:2181'))
    else:
        raise Exception('Unrecognized subscription holder mode: %s', mode)

def create_subscription_as_dict(topic, callback):
    return {
        'topic': topic,
        'callback': callback
    }