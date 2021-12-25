#------------------standard packages-----------------
import numpy as np
import pandas as pd
import datetime as dt
from tqdm.notebook import tqdm

#----------------caio's packages---------------------
import sys, os #add code folder to sys.path
sys.path.append(os.path.abspath(os.path.join('..', 'code')))

from cricket_read import cric_read
import cricket_metrics
from cricket_metrics import *

metrics=[getattr(cricket_metrics,m) for m in dir(cricket_metrics) if m not in ['__builtins__','__cached__','__doc__',
                                                          '__file__','__loader__','__name__','pd',
                                                          '__package__','__spec__','dt','np','wicket_types']]

#----------------------------get metrics----------------------------
def get_metrics(match_stack,player_list,form=30,aggregate=False):
    
    form_stack=match_stack[match_stack['start_date']>match_stack['start_date'].max()-dt.timedelta(days=form)]
    
    def player_row(player):
        row_dict={}
        
        if player=='all':
            row_dict['player']='all'
            for metric in metrics:
                row_dict[metric.__name__]=metric(match_stack,player_list)
                row_dict[metric.__name__+'_form']=metric(form_stack,player_list)
        else:
            row_dict['player']=player
            for metric in metrics:
                row_dict[metric.__name__]=metric(match_stack,[player])
                row_dict[metric.__name__+'_form']=metric(form_stack,[player])
                
        return row_dict
    if aggregate==True:
        return pd.DataFrame.from_dict([player_row('all')]).drop('player',axis=1)
    else:
        return pd.DataFrame.from_dict([player_row(player) for player in tqdm(player_list+['all'])])

#--------------------cricket data wrangling-------------------
def cric_wrangle(match_stack,results,hist_data_length,form=30,filename='cricket_data'):
    
    def match_row(match_id):
        row_dict={}
        
        match=match_stack[match_stack['match_id']==match_id]
#---------------------match_info--------------------   
        row_dict['match_id']=match_id
        row_dict['date']=match['start_date'].iloc[0]
        row_dict['set_team']=match['batting_team'].iloc[0]
        row_dict['chase_team']=match['bowling_team'].iloc[0]
        row_dict['set_team_win']=results.loc[results['match_id']==match_id,
                                    'result'].iloc[0]==row_dict['set_team']
        row_dict['set_team_toss']=results.loc[results['match_id']==match_id,
                                    'toss_winner'].iloc[0]==row_dict['set_team']
        row_dict['1_in_wickets']=match[match['innings']==1]['wicket_type'].count()
        row_dict['1w_score']=sum(match['wicket_type'].notna()*(match['innings']==1)*match['n_balls'])
#------------filter for players in teams------------ 
        batters=match.loc[match['batting_team']==match['batting_team'].iloc[0],'striker']
        bowlers=match.loc[match['bowling_team']==match['batting_team'].iloc[0],'bowler']
        set_team=list(np.unique(np.concatenate([batters,bowlers])))

        batters=match.loc[match['batting_team']==match['bowling_team'].iloc[0],'striker']
        bowlers=match.loc[match['bowling_team']==match['bowling_team'].iloc[0],'bowler']
        chase_team=list(np.unique(np.concatenate([batters,bowlers])))
        
        sub_stack=pd.concat([match_stack[match_stack['striker'].isin(set_team+chase_team)],
                    match_stack[match_stack['bowler'].isin(set_team+chase_team)]]).drop_duplicates()  
#-------filter for games before current match-------
        hist_start_date=row_dict['date']-dt.timedelta(days=365*hist_data_length)
    
        sub_stack=sub_stack[sub_stack['start_date'].between(hist_start_date,row_dict['date'],inclusive='left')] 
#---------------------get metrics-------------------
        row_metrics=get_metrics(sub_stack,set_team,form=form,aggregate=True)-get_metrics(
            sub_stack,chase_team,form=form,aggregate=True)
        
        for metric in row_metrics.columns:
            row_dict[metric]=row_metrics[metric][0]
        
        return row_dict
#-------------------produce dataset-----------------   
    match_list=match_stack['match_id'].unique()
    
    data=pd.DataFrame.from_dict([match_row(match_id) for match_id in tqdm(match_list)])
    
    hist_threshold=data['date'].min()+dt.timedelta(days=366*hist_data_length)
    data=data[data['date']>hist_threshold]
    data=data[~data.isin([np.nan, -np.inf,np.inf]).any(1)]
    
    data.to_csv('../data/'+filename+'.csv')
    return data