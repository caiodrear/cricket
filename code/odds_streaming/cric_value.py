#--------------standard packages------------------
import pandas as pd
import numpy as np
from thefuzz import fuzz
from thefuzz import process
import datetime as dt

#--------------streaming packages-----------------
import betfairlightweight

#--------------------get odds---------------------
def get_odds(match_odds_book,market_dict,prob_dict):
    
    odds_dict = {}
    for runner in match_odds_book.runners:
        runner_dict = {}
        runner_name = market_dict[match_odds_book.market_id]['runners'][runner.selection_id]
        odds_dict[runner_name] = runner_dict
        
        if runner.ex.available_to_back is None: #TODO: fix list index out of range bug
            runner_dict['back_odds'] = np.NaN
        else:
            runner_dict['back_odds'] = runner.ex.available_to_back[0].price
            
        if runner.ex.available_to_lay is None:
            runner_dict['lay_odds'] = np.NaN
        else:
            runner_dict['lay_odds'] = runner.ex.available_to_lay[0].price
            
        if prob_dict is not None:
            
            prob_name = process.extractOne(runner_name, prob_dict.keys(),
                                           scorer = fuzz.token_sort_ratio,score_cutoff = 0)[0]

            runner_dict['probability'] = prob_dict[prob_name]
        else:
            runner_dict['probability'] = np.NaN
        
        back_value = np.round((runner_dict['back_odds']*runner_dict['probability']/100-1)*100,2)
        lay_value = np.round((1-runner_dict['lay_odds']*runner_dict['probability']/100)/
                             (runner_dict['lay_odds']-1)*100,2)
        
        runner_dict['value'] = max(back_value,lay_value)

        if runner_dict['value'] == back_value:
            runner_dict['side'] = 'back'
        else:
            runner_dict['side'] = 'lay'
            
    return odds_dict

#--------------------get lines--------------------
def get_lines(runs_lines_book, market_dict, odds_score, proj_runs):
    
    if odds_score is None:
        return None
    
    market_name = market_dict[runs_lines_book.market_id]['name']
    inning = market_name.split(' ')[1]
    team = market_name.split(' ')[0]
    
    if runs_lines_book.runners[0].ex.available_to_back is None:
        under_line = np.NaN
    else:
        under_line = runs_lines_book.runners[0].ex.available_to_back[0].price
        
    if runs_lines_book.runners[0].ex.available_to_lay is None:
        over_line = np.NaN
    else:
        over_line = runs_lines_book.runners[0].ex.available_to_lay[0].price
    
    lines_dict = {team:{'inning':'', 'score':'', 'projected_runs':proj_runs,
                        'under_line':under_line,'over_line':over_line}}
    if inning == '1st':
        if float(odds_score[0][team]['overs']) > 0:
            
            lines_dict[team]['inning'] = 1
            lines_dict[team]['score'] = odds_score[0][team]['runs'] + '/' + odds_score[0][team]['wickets']
            
            return lines_dict
    
    elif inning == '2nd':
        if float(odds_score[1][team]['overs']) > 0:
            
            lines_dict[team]['inning'] = 2
            lines_dict[team]['score'] = odds_score[1][team]['runs'] + '/' + odds_score[1][team]['wickets']
            
            return lines_dict
       
    return None

#----------------process markets------------------
def process_markets(market_books, market_dict, odds_score, prob_dict, proj_runs):
    
    updates_dict, odds_dict, lines_dict = None, None, None
    
    for market_book in market_books:
        if market_dict[market_book.market_id]['name'] == 'Match Odds':
            
            updates_dict = {'publish_time':market_book.publish_time,
                            'market_status':market_book.status,
                            'inplay_status':str(market_book.inplay)}
            
            odds_dict = get_odds(market_book,market_dict,prob_dict)
            
        else:
            active_line = get_lines(market_book, market_dict, odds_score, proj_runs)
            if active_line is not None:
                lines_dict = active_line

    return updates_dict, odds_dict, lines_dict

#-----------------print markets-------------------
def print_markets(updates_dict, odds_dict, lines_dict):
    
    if updates_dict is not None:
        print(updates_dict['publish_time'])
        print('market status: ' + updates_dict['market_status'], '|' ,
              'inplay status: ' + updates_dict['inplay_status'])
        print('-'*65)
        
    if lines_dict is not None:
        print(pd.DataFrame.from_dict(lines_dict, orient = 'index'))
        print('-'*65)
        
    if odds_dict is not None:
        print(pd.DataFrame.from_dict(odds_dict, orient = 'index'))
        print('-'*65)
