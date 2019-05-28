import logging
import unittest

import koolie.config.items

_logger = logging.getLogger(__name__)


class TestLoad(unittest.TestCase):

    # def test_single_read(self):
    #     items = koolie.config.items.Load()
    #     items.raw_load('single_item.yaml')
    #     self.assertEqual(len(items.get_items()), 1)
    #
    # def test_multiple_read(self):
    #     items = koolie.config.items.Load()
    #     items.raw_load('multiple_item.yaml')
    #     self.assertEqual(len(items.get_items()), 2)
    #
    # def test_multiple_file_read(self):
    #     items = koolie.config.items.Load()
    #     items.raw_load('single_item.yaml', 'multiple_item.yaml')
    #
    #     self.assertEqual(len(items.get_items()), 3)
    #
    #     items.debug()

    def test_raw_load_items(self):
        items = koolie.config.items.RawItems()
        items.load('load_items.yaml')
        items.debug()

    def test_nginx_config(self):
        items = koolie.config.items.RawItems()
        items.load('base_nginx_config.yaml')
        items.debug()


if __name__ == '__main__':
    unittest.main()
