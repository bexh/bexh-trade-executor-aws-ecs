from sortedcontainers import SortedSet


class MockRedis:
    def __init__(self):
        self.kv_store = {}

    def zadd(self, name: str, mapping: map):
        for data, score in mapping.items():
            if name in self.kv_store:
                self.kv_store[name].add((data.encode("utf-8"), score))
            else:
                self.kv_store[name] = SortedSet([(data.encode("utf-8"), score)], key=lambda value: value[1])

    def zpopmin(self, name: str):
        if name in self.kv_store:
            try:
                return [self.kv_store[name].pop(index=0)]
            except IndexError:
                return []
        else:
            return []

    def zpopmax(self, name: str):
        if name in self.kv_store:
            try:
                return [self.kv_store[name].pop(index=-1)]
            except IndexError:
                return []
        else:
            return []

    def get(self, name: str):
        return self.kv_store.get(name)

    def delete(self, *names):
        for name in names:
            if name in self.kv_store:
                del self.kv_store[name]

    def set(self, name: str, value: str):
        self.kv_store[name] = value.encode("utf-8")

    def zrangebyscore(self, name: str, min: any, max: any, withscores: bool = False):
        if name in self.kv_store:
            values = list(self.kv_store[name].irange((None, min), (None, max)))
            if withscores:
                return values
            return list(map(lambda x: x[0], values))
        return []

    def zrem(self, name: str, *values: any):
        if name in self.kv_store:
            for value in values:
                matching_value = None
                for sorted_set_val in self.kv_store[name]:
                    if value.encode("utf-8") == sorted_set_val[0]:
                        matching_value = sorted_set_val
                        break
                if matching_value:
                    self.kv_store[name].remove(matching_value)
