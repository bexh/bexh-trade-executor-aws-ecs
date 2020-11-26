from json import dumps
from src.utils import get_current_utc_iso


class CancelBet:
    def __init__(
        self,
        event_id: str,
        sport: str,
        bet_id: int,
        brokerage_id: int,
        user_id: int,
        odds: int,
        on_team_abbrev: str
    ):
        self.event_id = event_id
        self.sport = sport
        self.bet_id = bet_id
        self.brokerage_id = brokerage_id
        self.user_id = user_id
        self.odds = odds
        self.on_team_abbrev = on_team_abbrev


class InactiveEvent:
    def __init__(
        self,
        event_id: str,
        sport: str,
        home_team_abbrev: str,
        away_team_abbrev: str,
        home_team_name: str,
        away_team_name: str,
        home_team_score: int,
        away_team_score: int,
        winning_team_abbrev: str,
        losing_team_abbrev: str,
        date: str
    ):
        self.event_id = event_id
        self.sport = sport
        self.home_team_abbrev = home_team_abbrev
        self.away_team_abbrev = away_team_abbrev
        self.home_team_name = home_team_name
        self.away_team_name = away_team_name
        self.home_team_score = home_team_score
        self.away_team_score = away_team_score
        self.winning_team_abbrev = winning_team_abbrev
        self.losing_team_abbrev = losing_team_abbrev
        self.date = date


class LimitBet:
    def __init__(
        self,
        event_id: str,
        sport: str,
        bet_id: int,
        brokerage_id: int,
        user_id: int,
        amount: float,
        odds: int,
        order_type: str,
        on_team_abbrev: str
    ):
        self.event_id = event_id
        self.sport = sport
        self.bet_id = bet_id
        self.brokerage_id = brokerage_id
        self.user_id = user_id
        self.amount = amount
        self.odds = odds
        self.order_type = order_type
        self.on_team_abbrev = on_team_abbrev


class MarketBet:
    def __init__(
        self,
        event_id: str,
        sport: str,
        bet_id: int,
        brokerage_id: int,
        user_id: int,
        amount: float,
        order_type: str,
        on_team_abbrev: str
    ):
        self.event_id = event_id
        self.sport = sport
        self.bet_id = bet_id
        self.brokerage_id = brokerage_id
        self.user_id = user_id
        self.amount = amount
        self.order_type = order_type
        self.on_team_abbrev = on_team_abbrev


class Bet:
    def __init__(
        self,
        event_id: str,
        sport: str,
        bet_id: int,
        brokerage_id: int,
        user_id: int,
        on_team_abbrev: str,
        amount: float = None,
        odds: int = None
    ):
        self.event_id = event_id
        self.sport = sport
        self.bet_id = bet_id
        self.brokerage_id = brokerage_id
        self.user_id = user_id
        self.on_team_abbrev = on_team_abbrev
        self.amount = amount
        self.odds = odds

    def __eq__(self, other):
        if (
            self.event_id == other.event_id and
            self.sport == other.sport and
            self.bet_id == other.bet_id and
            self.brokerage_id == other.brokerage_id and
            self.user_id == other.user_id and
            self.amount == other.amount and
            self.odds == other.odds and
            self.on_team_abbrev == other.on_team_abbrev
        ):
            return True
        return False

    @classmethod
    def fromlimitbet(cls, limit_bet: LimitBet):
        return cls(
            event_id=limit_bet.event_id,
            sport=limit_bet.sport,
            bet_id=limit_bet.bet_id,
            brokerage_id=limit_bet.brokerage_id,
            user_id=limit_bet.user_id,
            amount=limit_bet.amount,
            odds=limit_bet.odds,
            on_team_abbrev=limit_bet.on_team_abbrev
        )

    @classmethod
    def frommarketbet(cls, market_bet: MarketBet):
        return cls(
            event_id=market_bet.event_id,
            sport=market_bet.sport,
            bet_id=market_bet.bet_id,
            brokerage_id=market_bet.brokerage_id,
            user_id=market_bet.user_id,
            amount=market_bet.amount,
            on_team_abbrev=market_bet.on_team_abbrev
        )

    @classmethod
    def fromcancelbet(cls, cancel_bet: CancelBet):
        return cls(
            event_id=cancel_bet.event_id,
            sport=cancel_bet.sport,
            bet_id=cancel_bet.bet_id,
            brokerage_id=cancel_bet.brokerage_id,
            user_id=cancel_bet.user_id,
            on_team_abbrev=cancel_bet.on_team_abbrev
        )

    def better_than_or_equal(self, other, other_is_on_home: bool) -> bool:
        if other_is_on_home:
            return True if self.odds <= other.odds else False
        else:
            return True if other.odds >= self.odds else False

    def determine_amounts(self, other, other_is_on_home: bool) -> (float, float):
        bet_odds = other.odds * -1 if other_is_on_home else other.odds
        other_amount = round(self.amount * (bet_odds / 100), 2) if bet_odds > 0 else round(self.amount / (abs(bet_odds) / 100), 2)
        if other_amount < other.amount:
            return self.amount, other_amount
        else:
            bet_amount = round(other.amount * (abs(bet_odds) / 100), 2) if bet_odds < 0 else round(other.amount / (bet_odds / 100), 2)
            return bet_amount, other.amount

    def __repr__(self):
        bet_dict = {
            "event_id": self.event_id,
            "sport": self.sport,
            "bet_id": self.bet_id,
            "brokerage_id": self.brokerage_id,
            "user_id": self.user_id,
            "amount": self.amount,
            "odds": self.odds,
            "on_team_abbrev": self.on_team_abbrev
        }
        return dumps(bet_dict)

    def __str__(self):
        bet_dict = {
            "event_id": self.event_id,
            "sport": self.sport,
            "bet_id": self.bet_id,
            "brokerage_id": self.brokerage_id,
            "user_id": self.user_id,
            "amount": self.amount,
            "odds": self.odds,
            "on_team_abbrev": self.on_team_abbrev
        }
        return dumps(bet_dict)


class StatusDetails:
    def __init__(self, status: str = None, home_team_abbrev: str = None, away_team_abbrev: str = None):
        """
        :param status: Status = Literal["ACTIVE", "INACTIVE"]
        :param home_team_abbrev:
        :param away_team_abbrev:
        """
        self.status = status
        self.home_team_abbrev = home_team_abbrev
        self.away_team_abbrev = away_team_abbrev

    def __str__(self):
        dict_form = {
            "status": self.status,
            "home_team_abbrev": self.home_team_abbrev,
            "away_team_abbrev": self.away_team_abbrev
        }
        return dumps(dict_form)

    def __repr__(self):
        dict_form = {
            "status": self.status,
            "home_team_abbrev": self.home_team_abbrev,
            "away_team_abbrev": self.away_team_abbrev
        }
        return dumps(dict_form)


class ExecutedBet:
    def __init__(self, bet_id: int, brokerage_id: int, user_id: int, amount: float, status: str):
        """
        :param bet_id:
        :param brokerage_id:
        :param user_id:
        :param amount:
        :param status: BetStatus = Literal["EXECUTED", "PARTIALLY_EXECUTED", "CANCELLED", "EXPIRED_EVENT", "INSUFFICIENT_VOLUME"]
        """
        self.bet_id = bet_id
        self.brokerage_id = brokerage_id
        self.user_id = user_id
        self.amount = amount
        self.status = status

    @classmethod
    def frombet(cls, bet: Bet, status: str):
        """
        :param bet:
        :param status: BetStatus = Literal["EXECUTED", "PARTIALLY_EXECUTED", "CANCELLED", "EXPIRED_EVENT", "INSUFFICIENT_VOLUME"]

        :return:
        """
        return cls(
            bet_id=bet.bet_id,
            brokerage_id=bet.brokerage_id,
            user_id=bet.user_id,
            amount=bet.amount,
            status=status
        )

    def as_map(self) -> map:
        return {
            "bet_id": self.bet_id,
            "brokerage_id": self.brokerage_id,
            "user_id": self.user_id,
            "amount": self.amount,
            "status": self.status
        }


class ExecutedBets:
    def __init__(
        self,
        event_id: str,
        sport: str,
        bets: [ExecutedBet],
        odds: int = None,
        winning_team_abbrev: str = None
    ):
        self.event_id = event_id
        self.sport = sport
        self.odds = odds
        self.execution_time = get_current_utc_iso()
        self.bets = bets
        self.winning_team_abbrev = winning_team_abbrev

    def __str__(self):
        exec_bets_map = {
            "event_id": self.event_id,
            "sport": self.sport,
            "odds": self.odds,
            "execution_time": self.execution_time,
            "bets": list(map(lambda bet: bet.as_map(), self.bets)),
            "winning_team_abbrev": self.winning_team_abbrev
        }
        return dumps(exec_bets_map)

    @classmethod
    def frombets(
        cls,
        bet: Bet,
        bet_status: str,
        popped_bet: Bet,
        popped_bet_status: str,
        popped_bet_is_on_home: bool
    ):
        """
        :param bet:
        :param bet_status: BetStatus = Literal["EXECUTED", "PARTIALLY_EXECUTED", "CANCELLED", "EXPIRED_EVENT", "INSUFFICIENT_VOLUME"]
        :param popped_bet:
        :param popped_bet_status: BetStatus = Literal["EXECUTED", "PARTIALLY_EXECUTED", "CANCELLED", "EXPIRED_EVENT", "INSUFFICIENT_VOLUME"]

        :param popped_bet_is_on_home:
        :return:
        """
        transformed_bet = ExecutedBet.frombet(bet=bet, status=bet_status)
        transformed_popped_bet = ExecutedBet.frombet(bet=popped_bet, status=popped_bet_status)
        bets = [transformed_popped_bet, transformed_bet] if popped_bet_is_on_home else [transformed_bet, transformed_popped_bet]
        return cls(
            event_id=bet.event_id,
            sport=bet.sport,
            odds=popped_bet.odds,
            bets=bets
        )
