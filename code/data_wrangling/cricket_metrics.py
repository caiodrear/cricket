#------------------standard packages-----------------
import numpy as np
import pandas as pd
import datetime as dt

#----------------------------bat metrics----------------------------
#-----------------bat_avg------------------
def bat_avg(match_stack,player_list):
    
    n_runs=match_stack[match_stack['striker'].isin(player_list)]['runs_off_bat'].sum()

    n_times_dismissed=match_stack[match_stack['player_dismissed'].isin(player_list)]['player_dismissed'].count()
    
    if n_times_dismissed==0:
        return float("NaN")
    else:
        return n_runs/n_times_dismissed
    
#------------------bat_sr-------------------
def bat_sr(match_stack,player_list):
    
    player_matches=match_stack[match_stack['striker'].isin(player_list)]
    
    n_runs=player_matches['runs_off_bat'].sum()
    
    n_balls=player_matches[player_matches['noballs'].isna() & player_matches['wides'].isna()]['ball'].count()

    if n_balls==0:
        return float("NaN")
    else:
        return n_runs/n_balls*100
    
#---------------rel_bat_avg-----------------
def rel_bat_avg(match_stack,player_list):
    
    player_matches=match_stack[match_stack['striker'].isin(player_list)]
    
    match_list=list(player_matches['match_id'].unique())
    match_stack=match_stack[match_stack['match_id'].isin(match_list)]
    
    t_runs=match_stack['runs_off_bat'].sum()
    t_times_dismissed=match_stack['player_dismissed'].count()
    
    n_runs=player_matches['runs_off_bat'].sum()
    n_times_dismissed=match_stack[match_stack['player_dismissed'].isin(player_list)]['player_dismissed'].count()

    if t_runs==0 or n_times_dismissed==0:
        return float("NaN")
    else:
        return (n_runs*t_times_dismissed)/(n_times_dismissed*t_runs)*100
    
#---------------rel_bat_sr------------------
def rel_bat_sr(match_stack,player_list):
    
    player_matches=match_stack[match_stack['striker'].isin(player_list)]
    
    match_list=list(player_matches['match_id'].unique())
    match_stack=match_stack[match_stack['match_id'].isin(match_list)]
    
    t_runs=match_stack['runs_off_bat'].sum()
    t_balls=match_stack[match_stack['noballs'].isna() & match_stack['wides'].isna()]['ball'].count()
    
    n_runs=player_matches['runs_off_bat'].sum()
    n_balls=player_matches[player_matches['noballs'].isna() & player_matches['wides'].isna()]['ball'].count()

    if t_runs==0 or n_balls==0:
        return float("NaN")
    else:
        return (n_runs*t_balls)/(n_balls*t_runs)*100

#---------------------------bowl metrics----------------------------
wicket_types=['caught','bowled','lbw','caught and bowled','stumped','hit wicket']
#-----------------bowl_avg-----------------
def bowl_avg(match_stack,player_list):
    
    player_matches=match_stack[match_stack['bowler'].isin(player_list)]
    
    n_wickets=player_matches.loc[player_matches['wicket_type'].isin(wicket_types),'wicket_type'].count()

    n_runs_conceded=player_matches[['runs_off_bat','wides','noballs']].sum().sum()
    
    if n_wickets==0:
        return float("NaN")
    else:
        return n_runs_conceded/n_wickets
    
#-----------------economy------------------
def economy(match_stack,player_list):
    
    player_matches=match_stack[match_stack['bowler'].isin(player_list)]
    
    n_non_wides=player_matches[player_matches['wides'].isna()]
    n_balls=n_non_wides[n_non_wides['noballs'].isna()]['ball'].count()

    n_runs_conceded=player_matches[['runs_off_bat','wides','noballs']].sum().sum()
    
    if n_balls==0:
        return float("NaN")
    else:
        return n_runs_conceded/n_balls*6
    
#-----------------bowl_sr------------------
def bowl_sr(match_stack,player_list):
    
    player_matches=match_stack[match_stack['bowler'].isin(player_list)]
    
    n_non_wides=player_matches[player_matches['wides'].isna()]
    n_balls=n_non_wides[n_non_wides['noballs'].isna()]['ball'].count()

    n_wickets=player_matches.loc[player_matches['wicket_type'].isin(wicket_types),'wicket_type'].count()
    
    if n_wickets==0:
        return float("NaN")
    else:
        return n_balls/n_wickets

#---------------rel_economy----------------
def rel_economy(match_stack,player_list):
    
    player_matches=match_stack[match_stack['striker'].isin(player_list)]
    
    match_list=list(player_matches['match_id'].unique())
    match_stack=match_stack[match_stack['match_id'].isin(match_list)]
    
    n_non_wides=player_matches[player_matches['wides'].isna()]
    n_balls=n_non_wides[n_non_wides['noballs'].isna()]['ball'].count()
    n_runs_conceded=player_matches[['runs_off_bat','wides','noballs']].sum().sum()

    t_non_wides=match_stack[match_stack['wides'].isna()]
    t_balls=t_non_wides[t_non_wides['noballs'].isna()]['ball'].count()
    t_runs_conceded=match_stack[['runs_off_bat','wides','noballs']].sum().sum()

    if t_runs_conceded==0 or n_balls==0:
        return float("NaN")
    else:
        return (n_runs_conceded*t_balls)/(n_balls*t_runs_conceded)