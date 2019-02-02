from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.online.sls_reader import SlsReader
from ray.rllib.online.mock_sls_reader import MockSlsReader

__all__ = [
    "SlsReader",
    "MockSlsReader"
]