from aliyun.log.logitem import LogItem
from aliyun.log.logclient import LogClient
import time
from six.moves import queue
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.utils.annotations import override, PublicAPI


@PublicAPI
class SlsReader(object):
    @PublicAPI
    def __init__(self, config):
        # self._endpoint = config.get('endpoint')
        # self._accessKeyId = config.get("access_key_id")
        # self._accessKey = config.get("access_key")
        self._logStore = config.get("log_store")
        self._project = config.get("project")
        self._logClient = LogClient(
            config.get("endpoint"),
            config.get("access_key_id"),
            config.get("access_key"))
        self._batch_size_per_shard = config.get("batch_size_per_shard")    
        self._start_timestamp = config.get("start_timestamp")    
        self._shards = self._logClient.list_shards(self._project, self._logStore).get_shards_info()
        self._start_cursors = {}
        for shard in self._shards:
            shard_id = shard["shardID"]
            self._start_cursors.update(
                {shard_id : self._logClient.get_cursor(
                    self._project,
                    self._logStore,
                    shard_id,
                    self._start_timestamp).get_cursor()})           
        self._data_queue = queue.Queue()

    def next(self):
        for shard in self._shards:
            shard_id = shard["shardID"]
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
        
if __name__ == "__main__":
    conf = {
        "endpoint" : "cn-shanghai-ant-share.log.aliyuncs.com",
        "access_key_id" : "LTAILcBUr9lOpyfz",
        "access_key" : "3YYiBvOPZAGarcUtleGjZHLefMG9VJ",
        "project" : "ant-prizecore-lottery-received",
        "log_store" : "ant-prizecore-lottery-received",
        "batch_size_per_shard" : 10,
        "start_timestamp" : time.time() - 1000
    }

    sls_reader = SlsReader(conf)
    sls_reader.fetch_data()
    while not sls_reader._data_queue.empty():
        print(sls_reader._data_queue.get())
