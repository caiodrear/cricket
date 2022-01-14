#----------------standard packages------------------
import pandas as pd
from thefuzz import fuzz
from thefuzz import process
import numpy as np
import datetime as dt
#----------------streaming packages------------------
import logging
import betfairlightweight

#-----------------caio's modules---------------------
import sys, os #add code folder to sys.path
sys.path.append(os.path.abspath('../../config'))

from bf_stream import betfair_login, get_stream

from cric_odds import get_event, get_markets, get_score
from cric_value import get_odds, get_lines, process_markets, print_markets

#-----------------odds stream---------------------
def odds_stream(match_name, stream_mode = False):
    
#---------------------login-----------------------   
    trading = betfair_login()
    
#-----------------get market id-------------------

    event_name, event_dict, market_dict = get_event(match_name, trading)
    
    if market_dict is None:
        return None
    print(event_name)
    print('='*65)

#---------------------stream----------------------
    stream = get_stream(list(market_dict.keys()), trading, log_level=logging.WARNING)
    
#--------------create odds dataframe--------------
    market_books = {}
    while True:
        for market_book in stream.output_queue.get():
            market_books[market_book.market_id] = market_book
            
        odds_score = get_score(event_dict['event_id'], trading)

        prob_dict, proj_runs = None, np.NaN #add a dictionary of teams and probabilities, and a projected innings total

        updates_dict, odds_dict, lines_dict = process_markets(market_books.values(), market_dict, odds_score,
                                                              prob_dict, proj_runs)
        print_markets(updates_dict, odds_dict, lines_dict)

        if updates_dict['market_status'] != 'OPEN' or stream_mode == False: #TODO: error handling for updates_dict = None
            stream.stop()
            trading.logout()
            return updates_dict, odds_dict, lines_dict
#-------------------------------------------------
odds_stream('Australia vs England', stream_mode = True)