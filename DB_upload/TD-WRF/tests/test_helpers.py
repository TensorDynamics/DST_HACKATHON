"""
Tests for funcs/helpers
"""
import os
import unittest

from configs import path_config
from funcs import helpers


class TestjPickleIO(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestjPickleIO, self).__init__(*args, **kwargs)
        self.file_name = "test_obj_pickle"
        self.sample_data = list(range(1, 999))
        self.path = path_config.test_samples

    def test_a_file_exists(self):
        assert not os.path.isfile(os.path.join(self.path, self.file_name))

    def test_b_file_created(self):
        helpers.dump_obj_pickle(obj=self.sample_data, name=self.file_name, destination=self.path)
        assert os.path.isfile(os.path.join(self.path, f"{self.file_name}.pkl"))

    def test_c_read_obj(self):
        result = helpers.read_obj(destination=self.path, name=f"{self.file_name}.pkl")
        assert result == self.sample_data

    def test_d_remove_file(self):
        os.remove(path=os.path.join(self.path, f"{self.file_name}.pkl"))
        assert not os.path.isfile(os.path.join(self.path, f"{self.file_name}.pkl"))
