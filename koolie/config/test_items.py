import logging
import unittest

import koolie.config.items

_logger = logging.getLogger(__name__)


class TestLoad(unittest.TestCase):

    def test_items(self):
        items = koolie.config.items.Items()
        items.add_item({'type': 'foo'})

    def test_single_read(self):
        items = koolie.config.items.ReadItems()
        items.read('single_item.yaml')
        self.assertEqual(len(items.get_items()), 1)

    def test_multiple_read(self):
        items = koolie.config.items.ReadItems()
        items.read('multiple_item.yaml')
        self.assertEqual(len(items.get_items()), 2)

    def test_multiple_file_read(self):
        items = koolie.config.items.ReadItems()
        items.read('single_item.yaml', 'multiple_item.yaml')
        self.assertEqual(len(items.get_items()), 3)


if __name__ == '__main__':
    unittest.main()
