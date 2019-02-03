from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import glob
import json
import logging
import os
import random
import six
from six.moves.urllib.parse import urlparse

try:
    from smart_open import smart_open
except ImportError:
    smart_open = None

from ray.rllib.evaluation.sample_batch import MultiAgentBatch, SampleBatch, \
    DEFAULT_POLICY_ID
from ray.rllib.utils.compression import unpack_if_needed    

logger = logging.getLogger(__name__)

class MockSlsReader(object):
    def __init__(self, config, ioctx=None):
        self._ioctx = ioctx or IOContext()
        self._batch_size = config.get("batch_size")
        shards = os.path.join("/tmp/cartpole-out", "*.json")
        import numpy as np
        shard_split = np.array_split(np.asarray(range(len(shards))), ioctx.num_evaluators)
        self._shard_ids = []
        for i in shard_split[ioctx.worker_index]:
            self._shard_ids.append(shards[i])
        
        self.files = self._shard_ids
        logger.info("Sls Reader of worker: " + str(ioctx.worker_index) + " inited with Shards: " + str(self._shard_ids)) 
        self._batch_size_per_shard = int(self._batch_size / len(self._shard_ids)) + int(self._batch_size % len(self._shard_ids))
        logger.info("Sls Reader of worker: "
            + str(ioctx.worker_index) + " inited with batch size per shard: " + str(self._batch_size_per_shard)) 
      
    def next(self):
        batch = self._try_parse(self._next_line())
        tries = 0
        while not batch and tries < 100:
            tries += 1
            logger.debug("Skipping empty line in {}".format(self.cur_file))
            batch = self._try_parse(self._next_line())
        if not batch:
            raise ValueError(
                "Failed to read valid experience batch from file: {}".format(
                    self.cur_file))
        return self._postprocess_if_needed(batch)

    def _postprocess_if_needed(self, batch):
        if not self.ioctx.config.get("postprocess_inputs"):
            return batch

        if isinstance(batch, SampleBatch):
            out = []
            for sub_batch in batch.split_by_episode():
                out.append(self.ioctx.evaluator.policy_map[DEFAULT_POLICY_ID]
                           .postprocess_trajectory(sub_batch))
            return SampleBatch.concat_samples(out)
        else:
            # TODO(ekl) this is trickier since the alignments between agent
            # trajectories in the episode are not available any more.
            raise NotImplementedError(
                "Postprocessing of multi-agent data not implemented yet.")

    def _try_parse(self, line):
        line = line.strip()
        if not line:
            return None
        try:
            return _from_json(line)
        except Exception:
            logger.exception("Ignoring corrupt json record in {}: {}".format(
                self.cur_file, line))
            return None

    def _next_line(self):
        if not self.cur_file:
            self.cur_file = self._next_file()
        line = self.cur_file.readline()
        tries = 0
        while not line and tries < 100:
            tries += 1
            if hasattr(self.cur_file, "close"):  # legacy smart_open impls
                self.cur_file.close()
            self.cur_file = self._next_file()
            line = self.cur_file.readline()
            if not line:
                logger.debug("Ignoring empty file {}".format(self.cur_file))
        if not line:
            raise ValueError("Failed to read next line from files: {}".format(
                self.files))
        return line

    def _next_file(self):
        path = random.choice(self.files)
        if urlparse(path).scheme:
            if smart_open is None:
                raise ValueError(
                    "You must install the `smart_open` module to read "
                    "from URIs like {}".format(path))
            return smart_open(path, "r")
        else:
            return open(path, "r")

def _from_json(batch):
    if isinstance(batch, bytes):  # smart_open S3 doesn't respect "r"
        batch = batch.decode("utf-8")
    data = json.loads(batch)

    if "type" in data:
        data_type = data.pop("type")
    else:
        raise ValueError("JSON record missing 'type' field")

    if data_type == "SampleBatch":
        for k, v in data.items():
            data[k] = unpack_if_needed(v)
        return SampleBatch(data)
    elif data_type == "MultiAgentBatch":
        policy_batches = {}
        for policy_id, policy_batch in data["policy_batches"].items():
            inner = {}
            for k, v in policy_batch.items():
                inner[k] = unpack_if_needed(v)
            policy_batches[policy_id] = SampleBatch(inner)
        return MultiAgentBatch(policy_batches, data["count"])
    else:
        raise ValueError(
            "Type field must be one of ['SampleBatch', 'MultiAgentBatch']",
            data_type)



if __name__ == "__main__":
    conf = {
        "endpoint" : "",
        "access_key_id" : "",
        "access_key" : "",
        "project" : "",
        "log_store" : "",
        "batch_size" : 20,
        "batch_size_per_shard" : 10,
        "start_timestamp" : time.time() - 1000
    }

    sls_reader = MockSlsReader(conf)
    sls_reader.fetch_data()
    while not sls_reader._data_queue.empty():
        print(sls_reader._data_queue.get())        