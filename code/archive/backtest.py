import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import scipy.stats as scp

def backtest(test_data, ev_thresh = 0, prop = 0.05):
    
    bt_df = test_data.copy()
    
    bt_df['set_team_win'] = test_data['set_team'] == test_data['result']
    bt_df['prob_set'] = test_data['set_prob']
    bt_df['prob_chase'] = test_data['chase_prob']
    bt_df['set_team_odds']=test_data['set_odds']
    bt_df['chase_team_odds']=test_data['chase_odds'] 
    
    bt_df = bt_df[['set_team_win','prob_set','prob_chase','set_team_odds','chase_team_odds']].reset_index(drop = True)
    
#-------------------------------------------------backtest-------------------------------------------------------------

    commission=0.02
    
#------------------------------------------exponential backtest--------------------------------------------------------

    bankroll_init=1

    kelly_prop_set=(bt_df['set_team_odds']*bt_df['prob_set']-1)/(bt_df['set_team_odds']-1)
    kelly_prop_chase=(bt_df['chase_team_odds']*bt_df['prob_chase']-1)/(bt_df['chase_team_odds']-1)
    stake_prop=prop#np.mean([item for item in [max(l1,l2) for l1,l2 in zip(kelly_prop_set,kelly_prop_chase)] if item > 0])

    bankroll=[[],[],[],[]]
    bet_result=[0,0,0]

    for i in bt_df.index:

#----------------------------------------------------model-------------------------------------------------

        if bt_df['set_team_odds'][i]*bt_df['prob_set'][i]-1>ev_thresh:
            bet_result[0]=bt_df['set_team_odds'][i]*bt_df['set_team_win'][i]*(1-commission)-1
            kelly_prop=kelly_prop_set[i]

        elif bt_df['chase_team_odds'][i]*bt_df['prob_chase'][i]-1>ev_thresh:
            bet_result[0]=bt_df['chase_team_odds'][i]*(1-bt_df['set_team_win'][i])*(1-commission)-1
            kelly_prop=kelly_prop_chase[i]

        else:
            bet_result[0]=0
            kelly_prop = 0
#---------------------------------------------------back the favourite-------------------------------------    

        if bt_df['set_team_odds'][i]<bt_df['chase_team_odds'][i]:
            bet_result[1]=bt_df['set_team_odds'][i]*bt_df['set_team_win'][i]*(1-commission)-1

        else:
            bet_result[1]=bt_df['chase_team_odds'][i]*(1-bt_df['set_team_win'][i])*(1-commission)-1

#----------------------------------------------------lay the favourite--------------------------------------

        if bt_df['set_team_odds'][i]>bt_df['chase_team_odds'][i]:
            bet_result[2]=bt_df['set_team_odds'][i]*bt_df['set_team_win'][i]*(1-commission)-1

        else:
            bet_result[2]=bt_df['chase_team_odds'][i]*(1-bt_df['set_team_win'][i])*(1-commission)-1

#--------------------------------------------------bankroll calculations------------------------------------

        if i==0:
            bankroll[0].append(bankroll_init*(1+stake_prop*bet_result[0]))
            bankroll[3].append(bankroll_init*(1+kelly_prop*bet_result[0]))

            bankroll[1].append(bankroll_init*(1+stake_prop*bet_result[1]))
            bankroll[2].append(bankroll_init*(1+stake_prop*bet_result[2]))
        else:
            
            bankroll[0].append(bankroll[0][i-1]*(1+stake_prop*bet_result[0]))
            bankroll[3].append(bankroll[3][i-1]*(1+kelly_prop*bet_result[0]))

            bankroll[1].append(bankroll[1][i-1]*(1+stake_prop*bet_result[1]))
            bankroll[2].append(bankroll[2][i-1]*(1+stake_prop*bet_result[2]))

#-------------------------------------------------metrics------------------------------------------------------------        
    p_and_l=[[],[],[],[]]
    bet_returns=[0,0,0,0]
    confidence=[0,0,0,0]

    for i in [1,2]:
        bet_returns[i]=(bankroll[i][-1]-bankroll_init)/(sum(bankroll[i][:-1],bankroll_init)*stake_prop)*100

    bet_returns[0]=(bankroll[0][-1]-bankroll_init)/\
                   ((np.array([1]+bankroll[0][:-1])*\
                   np.ceil([max(l1,l2,0) for l1,l2 in zip(kelly_prop_set,kelly_prop_chase)])*stake_prop).sum())*100

    bet_returns[3]=(bankroll[3][-1]-bankroll_init)/\
                   ((np.array([1]+bankroll[3][:-1])*\
                   [max(l1,l2,0) for l1,l2 in zip(kelly_prop_set,kelly_prop_chase)]).sum())*100

    for i in range(len(bankroll)):

        p_and_l[i]=np.array(bankroll[i])
        p_and_l[i][1:] -= p_and_l[i][:-1]
        p_and_l[i]=[bet for bet in p_and_l[i] if bet != 0]

        confidence[i]=scp.t.cdf((bankroll[i][-1]-bankroll_init)/(np.sqrt(len(p_and_l[i]))*np.std(p_and_l[i],ddof=1)),\
                                len(p_and_l[i])-1)*100

#----------------------------------------------dataframe construction------------------------------------------------

    bt_metrics=pd.DataFrame.from_dict({'return':bet_returns,'t-confidence':confidence},orient='index',
                                     columns=['exp_model','exp_back_fav','exp_lay_fav','exp_model_kelly'])  

    bt_results=pd.DataFrame.from_dict({'exp_model':bankroll[0],'exp_back_fav':bankroll[1],
                                       'exp_lay_fav':bankroll[2],'exp_model_kelly':bankroll[3]})    

#----------------------------------------------linear backtest--------------------------------------------------------
    p_and_l=[[],[],[],[]]

    p_and_l[0]=(bt_df['set_team_odds']*bt_df['set_team_win']*(1-commission)-1)*\
                        (bt_df['set_team_odds']*bt_df['prob_set']-1>ev_thresh)+\
                        (bt_df['chase_team_odds']*(1-bt_df['set_team_win'])*(1-commission)-1)*\
                        (bt_df['chase_team_odds']*bt_df['prob_chase']-1>ev_thresh)

    p_and_l[1]=(bt_df['set_team_odds']*bt_df['set_team_win']*(1-commission)-1)*\
                            (bt_df['set_team_odds']<bt_df['chase_team_odds'])+\
                            (bt_df['chase_team_odds']*(1-bt_df['set_team_win'])*(1-commission)-1)*\
                            (bt_df['chase_team_odds']<=bt_df['set_team_odds'])

    p_and_l[2]=(bt_df['set_team_odds']*bt_df['set_team_win']*(1-commission)-1)*\
                            (bt_df['set_team_odds']>bt_df['chase_team_odds'])+\
                            (bt_df['chase_team_odds']*(1-bt_df['set_team_win'])*(1-commission)-1)*\
                            (bt_df['chase_team_odds']>=bt_df['set_team_odds'])

    bt_results['lin_model']=p_and_l[0].cumsum()
    bt_results['lin_back_fav']=p_and_l[1].cumsum()
    bt_results['lin_lay_fav']=p_and_l[2].cumsum()

#-----------------------------------------------------------metrics--------------------------------------------------  
    bet_returns=[0,0,0,0]
    confidence=[0,0,0,0]

    for i in range(3):
        bet_returns[i]=p_and_l[i].sum()/sum(p_and_l[i]!=0)*100

        confidence[i]=scp.t.cdf(p_and_l[i].sum()/(np.sqrt(len(p_and_l[i]))*np.std(p_and_l[i],ddof=1)),\
                                len(p_and_l[i])-1)*100

    bt_metrics['lin_model']=[bet_returns[0],confidence[0]]
    bt_metrics['lin_back_fav']=[bet_returns[1],confidence[1]]
    bt_metrics['lin_lay_fav']= [bet_returns[2],confidence[2]]   

#-----------------------------------------------------------charts--------------------------------------------------     

    bt_results.plot.line(y=['exp_model','exp_back_fav','exp_lay_fav'])
    plt.show()
    print(bt_metrics[['exp_model','exp_back_fav','exp_lay_fav']])
    
    bt_results.plot.line(y=['lin_model','lin_back_fav','lin_lay_fav'])
    plt.show()
    print(bt_metrics[['lin_model','lin_back_fav','lin_lay_fav']])

    return None
