class MockSlsReader(object):
    def __init__(self, config):
        pass
    
    def next(self):
        pass

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

    sls_reader = MockSlsReader(conf)
    sls_reader.fetch_data()
    while not sls_reader._data_queue.empty():
        print(sls_reader._data_queue.get())        