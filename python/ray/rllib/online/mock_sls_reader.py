import logging

logger = logging.getLogger(__name__)

class MockSlsReader(object):
    def __init__(self, config, ioctx=None):
        self._ioctx = ioctx or IOContext()
        self._batch_size = config.get("batch_size")
        shards = [1, 3, 5, 7, 9]
        import numpy as np
        shard_split = np.array_split(np.asarray(range(shards.len()), ioctx.num_evaluators)
        self._shard_ids = []
        for i in shard_split[ioctx.worker_index]:
            self._shard_ids.append(shards[i])
        
        logger.info("Sls Reader of worker: " + str(ioctx.worker_index) + " inited with Shards: " + str(self._shard_ids)) 
        self._batch_size_per_shard = self._batch_size / len(self._shard_ids) + self._batch_size % len(self._shard_ids)
        logger.info("Sls Reader of worker: "
            + str(ioctx.worker_index) + " inited with batch size per shard: " + str(self._batch_size_per_shard)) 
      
    def next(self):
        pass

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