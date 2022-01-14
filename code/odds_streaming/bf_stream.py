import os
import logging
import queue
import threading
from tenacity import retry, wait_exponential

import betfairlightweight
from betfairlightweight import StreamListener
from betfairlightweight import BetfairError
from betfairlightweight.filters import streaming_market_filter,streaming_market_data_filter

#-----------------caio's modules---------------------
import sys, os #add code folder to sys.path
sys.path.append(os.path.abspath('../../config'))
from config import username, password, app_key

#---------------------login-----------------------
def betfair_login():
    trading = betfairlightweight.APIClient(username, password, #login
                                           app_key = app_key, certs = os.path.abspath('../../config/certs'))
    trading.login()
    return trading

def get_stream(market_ids, trading, log_level = logging.WARNING):
#--------------------logging----------------------
    logging.basicConfig(level = log_level)
    logger = logging.getLogger(__name__)

#----------------error handling-------------------
    class Streaming(threading.Thread):
        def __init__(
            self,
            client: betfairlightweight.APIClient,
            market_filter: dict,
            market_data_filter: dict,
            conflate_ms: int = None,
            streaming_unique_id: int = 1000,
        ):
            threading.Thread.__init__(self, daemon = True, name = self.__class__.__name__)
            self.client = client
            self.market_filter = market_filter
            self.market_data_filter = market_data_filter
            self.conflate_ms = conflate_ms
            self.streaming_unique_id = streaming_unique_id
            self.stream = None
            self.output_queue = queue.Queue()
            self.listener = StreamListener(output_queue=self.output_queue)

        @retry(wait=wait_exponential(multiplier = 1, min = 2, max = 20))
        def run(self) -> None:
            logger.info("Starting MarketStreaming")
            self.client.login()
            self.stream = self.client.streaming.create_stream(
                unique_id = self.streaming_unique_id, listener = self.listener
            )
            try:
                self.streaming_unique_id = self.stream.subscribe_to_markets(
                    market_filter = self.market_filter,
                    market_data_filter = self.market_data_filter,
                    conflate_ms = self.conflate_ms,
                    initial_clk = self.listener.initial_clk,  #supplying these two values allows a reconnect
                    clk = self.listener.clk,
                )
                self.stream.start()
            except BetfairError:
                logger.error("MarketStreaming run error", exc_info=True)
                raise
            except Exception:
                logger.critical("MarketStreaming run error", exc_info=True)
                raise
            logger.info("Stopped MarketStreaming {0}".format(self.streaming_unique_id))

        def stop(self) -> None:
            if self.stream:
                self.stream.stop()

#--------------------filters----------------------
    market_filter = streaming_market_filter(market_ids = market_ids)
    market_data_filter = streaming_market_data_filter(fields = ["EX_BEST_OFFERS_DISP",'EX_MARKET_DEF'],
                                                              ladder_levels = 1)
#-------------------streaming---------------------
    streaming = Streaming(trading, market_filter, market_data_filter)
    streaming.start()

    return streaming