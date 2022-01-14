#----------------standard packages-------------------
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

#-------------------ML packages---------------------
from sklearn.preprocessing import StandardScaler

from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier as xgb
#import xgboost as xgb

from sklearn.metrics import classification_report, roc_curve, roc_auc_score, auc

#-------------------player model--------------------
def player_model(X_train, X_test, y_train, y_test, results, algorithm = 'forest', metrics = False):
#-------------------scaling-------------------------
    scaler = StandardScaler()
    X_train = pd.DataFrame(scaler.fit_transform(X_train), columns = X_train.columns)
    X_test = pd.DataFrame(scaler.fit_transform(X_test), columns = X_test.columns)
    
#------------------algorithms-----------------------      
    if algorithm == 'xgb':
        model = xgb.XGBClassifier(use_label_encoder = False, eval_metric = 'logloss')
        model.fit(X_train,y_train)
    else:
        model = RandomForestClassifier(n_estimators = 200, min_samples_split = 2, min_samples_leaf = 1,
                                       max_features = 'auto', max_depth = 100, bootstrap = True)
        model.fit(X_train, y_train)
    
#---------------value calculations------------------
    test_data = results[results.index.isin(y_test.index)].copy()
    test_data['set_prob'] = model.predict_proba(X_test)[:,1]
    test_data['chase_prob'] = model.predict_proba(X_test)[:,0]
    test_data['set_value'] = test_data['set_prob']*test_data['set_odds'] - 1
    test_data['chase_value'] = test_data['chase_prob']*test_data['chase_odds'] - 1

    if metrics == False:
        return model, test_data
    
#-------------------metrics-------------------------    
    y_pred = model.predict(X_test)
    print(classification_report(y_test,y_pred))

    false_auc = roc_auc_score(y_test, y_test.clip(upper = False))
    model_auc = roc_auc_score(y_test, test_data['set_prob'])
    
    print('chasing team always wins: roc auc = %.3f' % (false_auc))
    print('model prediction: roc auc = %.3f' % (model_auc))
    
    false_fpr, false_tpr, _ = roc_curve(y_test, y_test.clip(upper = False))
    model_fpr, model_tpr, _ = roc_curve(y_test, test_data['set_prob'])

    plt.plot(false_fpr, false_tpr, linestyle = '--', label = 'chasing team always wins')
    plt.plot(model_fpr, model_tpr, marker = '.', label = 'model prediction')

    plt.xlabel('false positive rate')
    plt.ylabel('true positive rate')
    plt.legend()
    plt.show()
    
    if model == 'xgb':
        xgb.plot_importance(clf,importance_type='weight')
        plt.show()
        print(pd.DataFrame.from_dict(clf.get_booster().get_fscore(),
                                     orient = 'index').sort_values(0, ascending = False).head(10))  
    return model, test_data