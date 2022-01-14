#------------------standard packages-----------------
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm

#-----------------caio's modules---------------------
import sys, os #add code folder to sys.path
sys.path.append(os.path.abspath('code/data_processing'))

from cricsheet_read import cricsheet_read #data processing

#--------------cricket metric functions--------------
import cricket_metrics
from cricket_metrics import *

metrics = [getattr(cricket_metrics,m) for m in dir(cricket_metrics)
           if m not in ['__builtins__','__cached__','__doc__','__file__',
                        '__loader__','__name__','__package__','__spec__',
                        'dt','np','pd','wicket_types']]

#------------------player wrangle--------------------
def player_wrangle(match_stack, results, min_player_matches = 14):
    
    info_stack = results.copy()
    info_stack['set_team_win'] = info_stack['set_team'] == info_stack['result']
    info_stack['set_team_toss'] = info_stack['set_team'] == info_stack['toss_winner']
    info_stack['days_since_match'] = (dt.date.today() - info_stack['start_date']).dt.days

    info_stack = info_stack[['start_date', 'set_team_win', 'days_since_match', 'set_team_toss', 'league', 'venue']]

    bat_stack = match_stack[['match_id','innings', 'striker']].rename(columns = {'striker':'player'})
    bowl_stack = match_stack[['match_id', 'innings', 'bowler']].rename(columns = {'bowler':'player'})

    player_stack = pd.concat([bat_stack,bowl_stack]).drop_duplicates(['match_id', 'player'])
    
    n_matches = player_stack[['match_id', 'player']].groupby('player').count().rename(columns = {'match_id':'no'})
    player_stack = player_stack.merge(n_matches, left_on = 'player', right_on = n_matches.index)
    player_stack = player_stack[player_stack['no'] > min_player_matches].drop('no', axis = 1)
    
    player_stack = player_stack.pivot(index = 'match_id', columns = 'player', values = 'innings').fillna(0)
    player_stack = info_stack.join(player_stack).sort_index()
    
    player_stack.to_csv('data/player_data.csv')
    
#---------------------get metrics--------------------
def get_metrics(match_stack, player_list, form = 30, aggregate = False):

    form_stack = match_stack[match_stack['start_date']
                             > match_stack['start_date'].max() - dt.timedelta(days = form)]
    
    def player_row(player):
        row_dict = {}
        
        if player == 'all':
            row_dict['player'] = 'all'
            for metric in metrics:
                row_dict[metric.__name__] = metric(match_stack,player_list)
                row_dict[metric.__name__ + '_form'] = metric(form_stack,player_list)
        else:
            row_dict['player'] = player
            for metric in metrics:
                row_dict[metric.__name__] = metric(match_stack,[player])
                row_dict[metric.__name__ + '_form'] = metric(form_stack,[player])
                
        return row_dict
    if aggregate == True:
        return pd.DataFrame.from_dict([player_row('all')]).drop('player',axis=1)
    
    return pd.DataFrame.from_dict([player_row(player) for player in tqdm(player_list + ['all'])])

#--------------=-------fe wrangle--------------------
def fe_wrangle(match_stack, results, hist_data_length = 2, form = 30):
    
    results = pd.DataFrame.to_dict(results, orient = 'index')
    def match_row(match_id):
        row_dict = {}
        
        match = match_stack[match_stack['match_id'] == match_id]
#---------------------match_info--------------------   
        row_dict['match_id'] = match_id
        row_dict['start_date'] = match['start_date'].iloc[0]
        row_dict['set_team_win'] = results[match_id]['result'] == match['batting_team'].iloc[0]
        row_dict['set_team_toss'] = results[match_id]['toss_winner'] == match['bowling_team'].iloc[0]
#------------filter for players in teams------------ 
        batters = match.loc[match['batting_team'] == match['batting_team'].iloc[0],'striker']
        bowlers = match.loc[match['bowling_team'] == match['batting_team'].iloc[0],'bowler']
        set_team = list(np.unique(np.concatenate([batters,bowlers])))

        batters = match.loc[match['batting_team'] == match['bowling_team'].iloc[0],'striker']
        bowlers = match.loc[match['bowling_team'] == match['bowling_team'].iloc[0],'bowler']
        chase_team = list(np.unique(np.concatenate([batters,bowlers])))
        
        sub_stack = pd.concat([match_stack[match_stack['striker'].isin(set_team + chase_team)],
                               match_stack[match_stack['bowler'].isin(set_team + chase_team)]]).drop_duplicates()  
#-------filter for games before current match-------
        hist_start_date = row_dict['start_date'] - dt.timedelta(days = 365*hist_data_length)
    
        sub_stack = sub_stack[sub_stack['start_date'].between(hist_start_date, row_dict['start_date'], inclusive='left')]

#---------------------get metrics-------------------

        row_metrics = get_metrics(sub_stack, set_team, form = form, aggregate=True) - get_metrics(
            sub_stack,chase_team, form = form, aggregate = True)
        
        for metric in row_metrics.columns:
            row_dict[metric] = row_metrics[metric][0]
        
        return row_dict
#-------------------produce dataset-----------------   
    match_list = match_stack[match_stack['start_date'] > #first match has no historical data
                             match_stack['start_date'].min()]['match_id'].unique()
    
    data = pd.DataFrame.from_dict([match_row(match_id) for match_id in tqdm(match_list)])
    
    hist_threshold = data['start_date'].min() + dt.timedelta(days=366*hist_data_length)
    data = data[data['start_date'] > hist_threshold]
    data = data[~data.isin([np.nan, -np.inf,np.inf]).any(1)].reset_index(drop = True).set_index('match_id')
    
    data.to_csv('data/fe_data.csv')
    
#-------------------data wrangle---------------------
def data_wrangle(model = 'all'): 
    match_stack, results = cricsheet_read()
    if model == 'player':
        player_wrangle(match_stack, results)
    elif model == 'fe':
        fe_wrangle(match_stack, results)
    else:
        player_wrangle(match_stack, results)
        fe_wrangle(match_stack, results)
#----------------------------------------------------
#data_wrangle(model = 'all')