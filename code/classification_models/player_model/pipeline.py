#------------------standard packages-----------------
import numpy as np
import pandas as pd
import datetime as dt

#-------------------ML packages---------------------
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OneHotEncoder

from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier as xgb
#import xgboost as xgb

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
    player_stack = info_stack.join(player_stack)
    
    player_stack = player_stack.sort_values('days_since_match', ascending = False)
    
    return player_stack

#-------------------player model---------------------
def player_model(match_data, results, test_date, algorithm = 'forest'):
#-----------------preprocessing---------------------
    X = match_data.drop(['set_team_win'], axis = 1).reset_index()
    y = match_data[['set_team_win', 'start_date']]
    non_enconding = ['set_team_toss', 'days_since_match', 'start_date']

    encoder = OneHotEncoder()
    X = X[non_enconding].join(pd.DataFrame(encoder.fit_transform(X.drop(non_enconding, axis = 1)).toarray()))
    X.columns = X.columns.map(str)

    X_train = X[X['start_date'] < test_date].copy().drop('start_date', axis = 1)
    X_test = X[X['start_date'] >= test_date].copy().drop('start_date', axis = 1)

    y_train = y[y['start_date'] < test_date].copy()['set_team_win']
    y_test =  y[y['start_date'] >= test_date].copy()['set_team_win']
    
    test_data = results[results.index.isin(y_test.index)].copy()
    
    scaler = StandardScaler()
    X_train = pd.DataFrame(scaler.fit_transform(X_train), columns = X_train.columns)
    X_test = pd.DataFrame(scaler.fit_transform(X_test), columns = X_test.columns)
    
#------------------algorithms-----------------------      
    if algorithm == 'xgb':
        clf = xgb.XGBClassifier(use_label_encoder = False, eval_metric = 'logloss')
        clf.fit(X_train,y_train)
    else:
        clf = RandomForestClassifier(n_estimators = 200, min_samples_split = 2, min_samples_leaf = 1,
                                     max_features = 'auto', max_depth = 100, bootstrap = True)
        clf.fit(X_train, y_train)
    
    test_data['set_prob'] = clf.predict_proba(X_test)[:,1]
    test_data['chase_prob'] = clf.predict_proba(X_test)[:,0]
    
#---------------value calculations------------------

    test_data['set_value'] = test_data['set_prob']*test_data['set_odds'] - 1
    test_data['chase_value'] = test_data['chase_prob']*test_data['chase_odds'] - 1

    return test_data