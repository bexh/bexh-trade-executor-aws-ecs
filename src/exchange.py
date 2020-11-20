from src.priority_queue import PriorityQueue
from src.domain_model import Bet, StatusDetails
from json import loads


class Exchange:
    def __init__(self, redis_client):
        self._pq = PriorityQueue(redis_client=redis_client)
        self._r = redis_client

    def submit_bet(self, bet: Bet):
        queue_name = f"pq:{bet.event_id}:{bet.on_team_abbrev}"
        self._pq.push(
            queue_name=queue_name,
            score=bet.odds,
            data=str(bet)
        )

    def pop_bet(self, event_id: str, team_abbrev: str, is_home_team: bool) -> Bet:
        queue_name = f"pq:{event_id}:{team_abbrev}"
        if is_home_team:
            values = self._pq.pop_min(queue_name=queue_name)
        else:
            values = self._pq.pop_max(queue_name=queue_name)

        value = next(iter(values), None)
        if not value:
            return None

        data, score = value
        bet = Bet(**loads(data.decode("utf-8")))
        return bet

    def get_status(self, event_id: str) -> StatusDetails:
        res = self._r.get(name=f"event:{event_id}")
        status_details = StatusDetails(**loads(res.decode("utf-8"))) if res else None
        return status_details

    def remove_bet(self, bet: Bet) -> Bet:
        queue_name = f"pq:{bet.event_id}:{bet.on_team_abbrev}"
        values = self._pq.get_items_in_range(queue_name=queue_name, min_score=bet.odds, max_score=bet.odds)
        for value in values:
            data, score = value
            potential_match_bet = Bet(**loads(data.decode("utf-8")))
            if potential_match_bet.bet_id == bet.bet_id:
                self._pq.remove_items(queue_name, str(potential_match_bet))
                return potential_match_bet
        return None
