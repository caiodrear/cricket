#--------------standard packages------------------
import pandas as pd
import numpy as np
from thefuzz import fuzz
from thefuzz import process
import datetime as dt

#--------------streaming packages-----------------
import betfairlightweight
from betfairlightweight import filters

#-----------------get event_id--------------------
def get_event(event_name, trading):

    event_dict = {}
    for event in trading.betting.list_events(filter = filters.market_filter(text_query = "Cricket")):
        if event.event.open_date < dt.datetime.today() + dt.timedelta(days=7):
            event_dict[event.event.name] = {'event_id':event.event.id, 'start_date':event.event.open_date}
            
    event_name = process.extractOne(event_name, event_dict.keys(),
                                scorer = fuzz.token_sort_ratio,score_cutoff = 50)[0]
    
    if event_name == None:
        print('match not found on betfair')
        trading.logout()
        return None, None
    else:
        market_dict = get_markets(event_dict[event_name]['event_id'],trading)
        trading.logout()
        return event_name, event_dict[event_name], market_dict

def get_markets(event_id, trading):    
#-------------list available markets--------------
    match_odds = trading.betting.list_market_catalogue(filter = filters.market_filter(
        event_ids = [event_id],
        market_type_codes = ['MATCH_ODDS']),
        market_projection = ["MARKET_START_TIME","RUNNER_DESCRIPTION"], max_results = 1000)[0]
    
    runs_lines = trading.betting.list_market_catalogue(filter = filters.market_filter(
        event_ids = [event_id],
        market_type_codes = ['1ST_INNINGS_RUNS_A', '1ST_INNINGS_RUNS_B',
                             '2ND_INNINGS_RUNS_A', '2ND_INNINGS_RUNS_B']),
        market_projection = ["MARKET_START_TIME","RUNNER_DESCRIPTION"], max_results = 1000)
    
#---------------add markets to dict---------------
    market_dict = {}
    market_dict[match_odds.market_id] = {'name':'Match Odds', 'runners':{}}
    
    for runner in match_odds.runners:
        market_dict[match_odds.market_id]['runners'][runner.selection_id] = runner.runner_name
    
    runner_0 = match_odds.runners[0].runner_name
    runner_1 = match_odds.runners[1].runner_name
    
    for market in runs_lines:
        for market_name in [runner_0 + ' 1st Innings Runs Line', runner_1 + ' 1st Innings Runs Line',
                            runner_0 + ' 2nd Innings Runs Line', runner_1 + ' 2nd Innings Runs Line']:
            if market.market_name == market_name:
                market_dict[market.market_id] = {'name':market_name, 'runners':{}}
                
                for runner in market.runners:
                    market_dict[market.market_id]['runners'][runner.selection_id] = runner.runner_name

    return market_dict

#--------------------get score--------------------
def get_score(event_id, trading):

    def inn_dict(team_inning):
        inn_dict = {}
        if team_inning is None:
            inn_dict['overs'] = '0.0'
            inn_dict['runs'] = '0'
            inn_dict['wickets'] = '0'
        else:
            inn_dict['overs'] = team_inning.overs
            inn_dict['runs'] = team_inning.runs
            if team_inning.wickets == 'ALL_OUT':
                inn_dict['wickets'] = '10'
            else:
                inn_dict['wickets'] = team_inning.wickets

        return inn_dict
    
    score = trading.in_play_service.get_scores(event_ids=[event_id])
    if score == []:
        return None

    score_dict = [{score[0].score.home.name:{},score[0].score.away.name:{}},
                  {score[0].score.home.name:{},score[0].score.away.name:{}}]
    for team in [score[0].score.home, score[0].score.away]:
        score_dict[0][team.name] = inn_dict(team.inning1)
        score_dict[1][team.name] = inn_dict(team.inning2)

    return score_dict