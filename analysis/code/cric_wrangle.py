#------------------standard packages-----------------
import numpy as np
import pandas as pd
import datetime as dt
from tqdm.notebook import tqdm
from thefuzz import fuzz, process

#--------------------player search-------------------
def get_player_names(match_stack, players, score_cutoff = 95):

    name_list = []
    for player in players:
        
        player_list = pd.concat([match_stack['striker'],match_stack['non_striker'],
                                 match_stack['bowler']])

        name = process.extractOne(player, player_list.unique(),
                                  scorer = fuzz.ratio, score_cutoff = score_cutoff)
        if name == None:
            surname = player.split(' ')[-1]
            print(player, 'not found')
            print('-'*20)
            print(player_list[player_list.str.find(surname)>-1].unique())
            print('-'*20)
            
        else:
            name_list.append(name[0])
            
    return name_list

#-------------------player matrices------------------
def bat_matrix(match_stack, players, date = dt.datetime.today()):
    
    player_stack = match_stack[(match_stack['striker'].isin(players)) &
                              ((match_stack['wides'].isna() == True) | 
                               (match_stack['out'] == True))].copy()
    
    player_stack['days_since_match'] = (date - player_stack['start_date']).dt.days
    
    player_stack = player_stack[['days_since_match','ball_no','runs', 'wickets', 'runs_off_bat', 'out']]
    
    return player_stack.sort_values(['days_since_match', 'ball_no']).reset_index(drop = True)

def bowl_matrix(match_stack, players, date = dt.datetime.today()):
    
    player_stack = match_stack[(match_stack['bowler'].isin(players))].copy()
    
    player_stack['wide'] = player_stack['wides'].notna()
    player_stack['no_ball'] = player_stack['noballs'].notna()
    player_stack['runs_conceded'] =  player_stack['runs_off_bat']# + player_stack['extras'] 
    
    player_stack['days_since_match'] = (date - player_stack['start_date']).dt.days
    
    player_stack = player_stack[['days_since_match','ball_no','runs', 'wickets', 'runs_conceded', 'out',
                                 'wide', 'no_ball']]
    
    return player_stack.sort_values(['days_since_match', 'ball_no']).reset_index(drop = True)