from src.exchange import Exchange
from src.logger import Logger
from src.domain_model import Bet, CancelBet, InactiveEvent, LimitBet, MarketBet, ExecutedBet, ExecutedBets
from json import loads
import copy


class BetExecutor:
    def __init__(self, logger: Logger, redis_client, kinesis_client, output_stream_name: str):
        self._exchange = Exchange(redis_client=redis_client)
        self._logger = logger
        self._kinesis_client = kinesis_client
        self._output_stream_name = output_stream_name

    def handle_record(self, record: map):
        data = loads(record['Data'].decode("utf-8"))
        action = data["action"]
        value = data["value"]

        if action == "NEW_LIMIT_BET":
            limit_bet = LimitBet(**value)
            self._handle_limit_bet(bet=limit_bet)
        elif action == "NEW_MARKET_BET":
            market_bet = MarketBet(**value)
            self._handle_market_bet(bet=market_bet)
        elif action == "INACTIVE_EVENT":
            inactive_event = InactiveEvent(**value)
            self._handle_inactive_event(event=inactive_event)
        elif action == "CANCEL_BET":
            cancel_bet = CancelBet(**value)
            self._handle_cancel_bet(bet=cancel_bet)
        else:
            self._logger.error(f"No valid matching action: {action}")

    def _handle_limit_bet(self, bet: LimitBet):
        generic_bet = Bet.fromlimitbet(limit_bet=bet)
        modified_bet = copy.deepcopy(generic_bet)
        status_details = self._exchange.get_status(event_id=bet.event_id)

        if status_details:
            self._logger.debug(f"Event status details: {status_details.status}, {status_details.home_team_abbrev}, {status_details.away_team_abbrev}")
        else:
            self._logger.debug(f"Status details do not exist for: {bet.event_id}")

        executed_bets = []

        if self._is_inactive_event(status_details):
            self._logger.debug(f"Expired event {status_details}")
            non_executed_bet = ExecutedBets(
                event_id=generic_bet.event_id,
                sport=generic_bet.sport,
                bets=[ExecutedBet.frombet(bet=generic_bet, status="CANCELLED")]
            )
            self._kinesis_client.put_record(
                StreamName=self._output_stream_name,
                Data=str(non_executed_bet),
                PartitionKey=non_executed_bet.event_id
            )
            return

        is_home_team = True if bet.on_team_abbrev == status_details.home_team_abbrev else False
        other_team_abbrev = status_details.home_team_abbrev if status_details.home_team_abbrev != bet.on_team_abbrev else status_details.away_team_abbrev

        while True:
            popped_bet = self._exchange.pop_bet(
                event_id=bet.event_id,
                team_abbrev=other_team_abbrev,
                is_home_team=not is_home_team
            )
            self._logger.debug(f"popped event: {str(popped_bet)}")
            if (popped_bet and modified_bet.better_than_or_equal(other=popped_bet, other_is_on_home=not is_home_team)
               and modified_bet.amount > 0):
                to_subtract_modified_bet, to_subtract_popped_bet = modified_bet.determine_amounts(other=popped_bet, other_is_on_home=not is_home_team)

                modified_bet.amount -= to_subtract_modified_bet
                popped_bet.amount -= to_subtract_popped_bet

                tmp_bet_copy = copy.deepcopy(modified_bet)
                tmp_bet_copy.amount = to_subtract_modified_bet

                tmp_popped_bet_copy = copy.deepcopy(popped_bet)
                tmp_popped_bet_copy.amount = to_subtract_popped_bet

                bet_status = "EXECUTED" if modified_bet.amount == 0 else "PARTIALLY_EXECUTED"
                popped_bet_status = "EXECUTED" if popped_bet.amount == 0 else "PARTIALLY_EXECUTED"

                executed_bet = ExecutedBets.frombets(
                    bet=tmp_bet_copy,
                    bet_status=bet_status,
                    popped_bet=tmp_popped_bet_copy,
                    popped_bet_status=popped_bet_status,
                    popped_bet_is_on_home=not is_home_team
                )

                executed_bets.append(executed_bet)

                # push the popped bet back on the exchange
                if popped_bet and popped_bet.amount > 0:
                    self._exchange.submit_bet(bet=popped_bet)
            else:
                # push the popped bet back on the exchange
                if popped_bet and popped_bet.amount > 0:
                    self._exchange.submit_bet(bet=popped_bet)
                break

        # push the modified bet to the exchange if out of viable bets on the queue
        if modified_bet.amount > 0:
            self._exchange.submit_bet(bet=modified_bet)

        for executed_bet in executed_bets:
            self._kinesis_client.put_record(
                StreamName=self._output_stream_name,
                Data=str(executed_bet),
                PartitionKey=executed_bet.event_id
            )

    def _handle_market_bet(self, bet: MarketBet):
        generic_bet = Bet.frommarketbet(market_bet=bet)
        modified_bet = copy.deepcopy(generic_bet)
        status_details = self._exchange.get_status(event_id=bet.event_id)

        if status_details:
            self._logger.debug(f"Event status details: {status_details.status}, {status_details.home_team_abbrev}, {status_details.away_team_abbrev}")
        else:
            self._logger.debug(f"Status details do not exist for: {bet.event_id}")

        executed_bets = []
        popped_bets = []

        if self._is_inactive_event(status_details):
            self._logger.debug(f"Expired event {status_details}")
            non_executed_bet = ExecutedBets(
                event_id=generic_bet.event_id,
                sport=generic_bet.sport,
                bets=[ExecutedBet.frombet(bet=generic_bet, status="CANCELLED")]
            )
            self._kinesis_client.put_record(
                StreamName=self._output_stream_name,
                Data=str(non_executed_bet),
                PartitionKey=non_executed_bet.event_id
            )
            return

        is_home_team = True if bet.on_team_abbrev == status_details.home_team_abbrev else False
        other_team_abbrev = status_details.home_team_abbrev if status_details.home_team_abbrev != bet.on_team_abbrev else status_details.away_team_abbrev

        while True:
            popped_bet = self._exchange.pop_bet(
                event_id=bet.event_id,
                team_abbrev=other_team_abbrev,
                is_home_team=not is_home_team
            )
            if popped_bet:
                popped_bets.append(copy.deepcopy(popped_bet))

            self._logger.debug(f"popped event: {str(popped_bet)}")
            if popped_bet and modified_bet.amount > 0:
                to_subtract_modified_bet, to_subtract_popped_bet = modified_bet.determine_amounts(other=popped_bet, other_is_on_home=not is_home_team)

                modified_bet.amount -= to_subtract_modified_bet
                popped_bet.amount -= to_subtract_popped_bet

                tmp_bet_copy = copy.deepcopy(modified_bet)
                tmp_bet_copy.amount = to_subtract_modified_bet

                tmp_popped_bet_copy = copy.deepcopy(popped_bet)
                tmp_popped_bet_copy.amount = to_subtract_popped_bet

                bet_status = "EXECUTED" if modified_bet.amount == 0 else "PARTIALLY_EXECUTED"
                popped_bet_status = "EXECUTED" if popped_bet.amount == 0 else "PARTIALLY_EXECUTED"

                executed_bet = ExecutedBets.frombets(
                    bet=tmp_bet_copy,
                    bet_status=bet_status,
                    popped_bet=tmp_popped_bet_copy,
                    popped_bet_status=popped_bet_status,
                    popped_bet_is_on_home=not is_home_team
                )

                executed_bets.append(executed_bet)

                # if the entire bet executed but some left on the last popped bet
                # throw what's left on popped bet back on exchange
                if modified_bet.amount == 0 and popped_bet.amount > 0:
                    self._exchange.submit_bet(bet=popped_bet)
                    break
            else:
                break

        # if entire bet executed send executed bets to kinesis out
        if modified_bet.amount == 0:
            for executed_bet in executed_bets:
                self._kinesis_client.put_record(
                    StreamName=self._output_stream_name,
                    Data=str(executed_bet),
                    PartitionKey=executed_bet.event_id
                )
        # if not enough volume on the other side to execute the market bet
        # pop all popped bets back on the exchange, and send bet status to kinesis
        else:
            for bet in popped_bets:
                self._exchange.submit_bet(bet=bet)
            non_executed_bet = ExecutedBets(
                event_id=generic_bet.event_id,
                sport=generic_bet.sport,
                bets=[ExecutedBet.frombet(bet=generic_bet, status="INSUFFICIENT_VOLUME")]
            )
            self._kinesis_client.put_record(
                StreamName=self._output_stream_name,
                Data=str(non_executed_bet),
                PartitionKey=non_executed_bet.event_id
            )

    def _handle_inactive_event(self, event: InactiveEvent):
        status_details = self._exchange.get_status(event_id=event.event_id)
        if not status_details:
            self._logger.error(f"No event details for handling inactive event {event.event_id}")
        else:
            self._logger.info(f"Handling inactive event for: {event.event_id}")

        # purge exchange on both sides
        for team_abbrev, is_home_team in [(event.home_team_abbrev, True), (event.away_team_abbrev, False)]:
            while True:
                popped_bet = self._exchange.pop_bet(
                    event_id=event.event_id,
                    team_abbrev=team_abbrev,
                    is_home_team=is_home_team
                )
                if popped_bet:
                    non_executed_bet = ExecutedBets(
                        event_id=event.event_id,
                        sport=event.sport,
                        bets=[ExecutedBet.frombet(bet=popped_bet, status="CANCELLED")]
                    )
                    self._kinesis_client.put_record(
                        StreamName=self._output_stream_name,
                        Data=str(non_executed_bet),
                        PartitionKey=non_executed_bet.event_id
                    )
                else:
                    break

        # poison pill for connector to close out all bets on the event
        # needs same schema as other bets for the purpose of kinesis analytics
        close_out_bets = ExecutedBets(
            event_id=event.event_id,
            sport=event.sport,
            winning_team_abbrev=event.winning_team_abbrev,
            bets=[ExecutedBet(
                bet_id=None,
                brokerage_id=None,
                user_id=None,
                amount=None,
                status="EXPIRED_EVENT"
            )]
        )
        self._kinesis_client.put_record(
            StreamName=self._output_stream_name,
            Data=str(close_out_bets),
            PartitionKey=event.event_id
        )

    def _handle_cancel_bet(self, bet: CancelBet):
        generic_bet = Bet.fromcancelbet(cancel_bet=bet)
        removed_bet = self._exchange.remove_bet(bet=generic_bet)
        if removed_bet is not None:
            non_executed_bet = ExecutedBets(
                event_id=bet.event_id,
                sport=bet.sport,
                bets=[ExecutedBet.frombet(bet=removed_bet, status="CANCELLED")]
            )
            self._kinesis_client.put_record(
                StreamName=self._output_stream_name,
                Data=str(non_executed_bet),
                PartitionKey=non_executed_bet.event_id
            )

    @staticmethod
    def _is_inactive_event(status_details):
        if not status_details or status_details.status == "INACTIVE":
            return True
        return False
