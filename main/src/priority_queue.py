class PriorityQueue:
    def __init__(self, redis_client):
        self._r = redis_client

    def push(self, queue_name: str, score: float, data: str):
        self._r.zadd(
            name=queue_name,
            mapping={data: score}
        )

    def pop_min(self, queue_name: str):
        return self._r.zpopmin(name=queue_name)

    def pop_max(self, queue_name: str):
        return self._r.zpopmax(name=queue_name)

    def get_items_in_range(self, queue_name: str, min_score: int, max_score: int):
        return self._r.zrangebyscore(name=queue_name, min=min_score, max=max_score, withscores=True)

    def remove_items(self, queue_name, *values):
        self._r.zrem(queue_name, *values)
