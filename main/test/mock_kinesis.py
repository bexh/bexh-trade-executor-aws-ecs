class MockKinesis:
    def __init__(self):
        self.streams = {}

    def put_record(self, StreamName: str, Data: any, PartitionKey: str):
        if StreamName in self.streams:
            self.streams[StreamName].append(Data)
        else:
            self.streams[StreamName] = [Data]
