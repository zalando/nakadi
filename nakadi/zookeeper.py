import json
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NodeExistsError

class ZookeeperClient(object):

    def __init__(self, zk_hosts):
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()

    def put(self, entity, id, data):
        path = '/{}/{}'.format(entity, id)
        binary_value = json.dumps(data).encode('utf-8')
        try:
            self.zk.create(path, binary_value, makepath=True)
            return True
        except NodeExistsError:
            self.zk.set(path, binary_value)
            return False

    def get(self, entity, id):
        path = '/{}/{}'.format(entity, id)
        try:
            binary_data, _ = self.zk.get(path)
        except NoNodeError:
            return None
        return json.loads(binary_data.decode('utf-8'))

    def delete(self, entity, id):
        path = '/{}/{}'.format(entity, id)
        try:
            self.zk.delete(path)
            return True
        except NoNodeError:
            return False

    def list(self, entity):
        path = '/{}'.format(entity)
        try:
            children = self.zk.get_children(path)
            for child in children:
                value = self.get(entity, child)
                yield (child, value)
        except NoNodeError:
            yield []