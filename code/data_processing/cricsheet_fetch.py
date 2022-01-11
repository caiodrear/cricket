
# this code takes the csv versions of the cricsheet.org data for t20i and league matches
# it produces two csvs: a stacked version of all the ball-by-ball (bbb) data and the result and toss of each match
# it adds a few new columns depicting game state (wickets, runs etc.) and removes games with no result, tie or super-over result.

#---------------standard packages------------------
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm
from pathlib import Path

#-----------------zip processors-------------------
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen

#-------------------csv readers--------------------
import csv
from io import TextIOWrapper

#----------------league dictionary-----------------
league_dict = {'ntb':'Vitality Blast', 'ipl':'Indian Premier League',
               'cpl':'Carribean Premier League', 'psl':'Pakistan Super League',
               'bbl':'Big Bash League', 't20s':'T20 Internationals'}
               
#----------------fetch bbb data--------------------
def cricsheet_fetch(url):

    zipfile = ZipFile(BytesIO(urlopen(url).read()))
    ziplist = zipfile.namelist()

    ziplist.remove('README.txt')
    if 'all_matches.csv' in ziplist:
        ziplist.remove('all_matches.csv')
    
    league = league_dict[url.split('_')[-3].split('/')[-1]]
    
    def file_row(file):
        row_dict = {}
        reader = csv.reader(TextIOWrapper(zipfile.open(file), 'utf-8'))
        row_dict['match_id'] = file.replace('_info.csv','')

        team_names = []
        for row in reader:
            if 'toss_winner' in row:
                 row_dict['toss_winner'] = row[-1]
            elif 'winner' in row:
                row_dict['result'] = row[-1]
            elif 'outcome' in row:
                row_dict['result'] = row[-1]
            elif 'team' in row:
                team_names.append(row[-1])
        row_dict['match_name'] = team_names[0] + ' v ' + team_names[1]
        return row_dict

    results = pd.DataFrame.from_dict([file_row(file) for file in ziplist
                                      if 'info' in file]).set_index('match_id').rename_axis(index=None)
    
    results = results[~results['result'].isin(['tie','no result'])] #remove draws

    match_stack = pd.concat([pd.read_csv(zipfile.open(file), dtype = {'ball':str, 'match_id':str},
                                         parse_dates = ['start_date']) for file in ziplist 
                             if 'info' not in file and file in results.index + '.csv'], ignore_index=True)

    match_stack.insert(1, 'league', league)
    
    out = (match_stack['player_dismissed'] == match_stack['striker']) | (
           match_stack['other_player_dismissed'] == match_stack['striker'])
    
    match_stack.insert(7, 'out', out)
    
#---------------game state features----------------
    ball_no = pd.DataFrame(match_stack['ball'].str.split('.').tolist()).astype('int')
    match_stack.insert(6, 'ball_no', ball_no[0]*6 + ball_no[1].clip(upper = 6))

    match_stack['runs'] = match_stack['runs_off_bat'] + match_stack['extras']
    match_stack['wickets'] = match_stack['wicket_type'].notna() + match_stack['other_wicket_type'].notna()
    
    game_state = match_stack.groupby(['match_id', 'innings']).cumsum()[['runs','wickets']]
    match_stack = match_stack.join(game_state, lsuffix = '_0')
    
    runs = match_stack.pop('runs')
    wickets = match_stack.pop('wickets')
    
    match_stack.insert(8, 'runs', runs - match_stack['runs_0'])
    match_stack.insert(9, 'wickets', wickets - match_stack['wickets_0'])
    match_stack.drop(['runs_0', 'wickets_0'], axis = 1, inplace = True)
     
#--------------------icc filter--------------------
    icc_teams=['Australia', 'England', 'Bangladesh',' India', 'Pakistan','South Africa',
               'New Zealand', 'West Indies', 'Sri Lanka', 'Afghanistan', 'Zimbabwe',
               'Netherlands','Scotland', 'Ireland']

    if league == 'T20 Internationals':
        match_stack = match_stack[match_stack['batting_team'].isin(icc_teams) & 
                                  match_stack['bowling_team'].isin(icc_teams)]
        
        results = results[results.index.isin(match_stack['match_id'].unique())]
        
    match_stack.reset_index(inplace = True, drop = True)

    results = results.merge(match_stack[match_stack['innings'] == 1].drop_duplicates('match_id'),
                            left_index = True, right_on = 'match_id').set_index('match_id')

    results = results[['start_date', 'league', 'venue', 'match_name',
                       'batting_team', 'bowling_team','toss_winner', 'result']].rename(columns = {'batting_team':'set_team',
                                                                                                  'bowling_team':'chase_team'})
    return match_stack, results

#-------------loop over leagues/t20i---------------
def multi_fetch(leagues = ['ntb', 'ipl', 'cpl', 'psl', 'bbl', 't20s']):
        
    match_stack_list, results_list = [], []
    for league in tqdm(leagues):
        match_stack, results = cricsheet_fetch('https://cricsheet.org/downloads/' + league + '_male_csv2.zip')
        match_stack_list.append(match_stack)
        results_list.append(results)

    pd.concat(match_stack_list,ignore_index = True).to_csv(Path('data/master/master_data.csv'),index = False)
    pd.concat(results_list).to_csv(Path('data/master/master_results.csv'))
    
#-------------------run script---------------------
multi_fetch()