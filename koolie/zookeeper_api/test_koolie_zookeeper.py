import unittest

from koolie.zookeeper_api.koolie_zookeeper import AbstractKoolieZooKeeper, UsingKazoo


class MyTestCase(unittest.TestCase):

    def test_using_kazoo(self):
        koolie_zookeeper: AbstractKoolieZooKeeper
        with UsingKazoo(ZOOKEEPER_HOSTS='localhost:2181') as koolie_zookeeper:
            for child in koolie_zookeeper.get_children('/'):
                print(child)
            koolie_zookeeper.create_ephemeral_node(path='/foo', value='bar'.encode('utf-8'))
            for child in koolie_zookeeper.get_children('/'):
                print(child)
            print(koolie_zookeeper.get_node_value(path='/foo').decode('utf-8'))
            print(koolie_zookeeper.create_uuid_node())
            for child in koolie_zookeeper.get_children('/'):
                print(child)


if __name__ == '__main__':
    unittest.main()
