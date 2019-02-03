from aliyun.log.logitem import LogItem
from aliyun.log.logclient import LogClient
import logging
import time
from six.moves import queue
from ray.rllib.offline import IOContext
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.utils.annotations import override, PublicAPI

logger = logging.getLogger(__name__)

@PublicAPI
class SlsReader(object):
    @PublicAPI
    def __init__(self, config, ioctx=None):
        self._ioctx = ioctx or IOContext()
        self._logStore = config.get("log_store")
        self._project = config.get("project")
        self._logClient = LogClient(
            config.get("endpoint"),
            config.get("access_key_id"),
            config.get("access_key"))
        self._batch_size = config.get("batch_size")
        self._start_timestamp = config.get("start_timestamp")
        shards = self._logClient.list_shards(self._project, self._logStore).get_shards_info()
        import numpy as np
        shard_split = np.array_split(np.asarray(range(shards.len())), ioctx.num_evaluators)
        self._shard_ids = []
        for i in shard_split[ioctx.worker_index]:
            self._shard_ids.append(shards[i]["shardID"])
        logger.info("Sls Reader of worker: " + str(ioctx.worker_index) + " inited with Shards: " + str(self._shard_ids))
        self._batch_size_per_shard = self._batch_size / self._shard_ids.len() + self._batch_size % self._shard_ids.len()
        logger.info("Sls Reader of worker: "
            + str(ioctx.worker_index) + " inited with batch size per shard: " + str(self._batch_size_per_shard))

        self._start_cursors = {}
        for shard_id in self._shard_ids:
            self._start_cursors.update(
                {shard_id : self._logClient.get_cursor(
                    self._project,
                    self._logStore,
                    shard_id,
                    self._start_timestamp).get_cursor()})           
        self._data_queue = queue.Queue()

    def next(self):
        if self._data_queue.qsize() < self._batch_size:
            for shard_id in self._shard_ids:
                start_cursor = self._start_cursors[shard_id]
                for i in range(self._batch_size_per_shard):
                    res = self._logClient.pull_logs(
                        self._project, self._logStore, shard_id, start_cursor)
                    if res.body["logs"]:
                        for i in res.body["logs"]:
                            self._data_queue.put((i['__time__'], i['content']))
                    next_cursor = res.get_next_cursor()
                    if next_cursor == start_cursor:
                        break
                    start_cursor = next_cursor
                self._start_cursors.update({shard_id:start_cursor})
        res = []
        for _ in range(self._batch_size):
            res.append(self._data_queue.get()) 
        return res           
        
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

    ioctx = IOContext(worker_index = 3, num_evaluators = 5)
    sls_reader = SlsReader(conf, ioctx)
    res = sls_reader.next()
    for record in res:
        print(record)
        
