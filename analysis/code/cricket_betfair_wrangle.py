import logging
from typing import List, Set, Dict, Tuple, Optional

from unittest.mock import patch
from itertools import zip_longest
import functools

import os
import tarfile
import zipfile
import bz2
import glob

import numpy as np
import pandas as pd
import betfairlightweight
from betfairlightweight import StreamListener
from tqdm.notebook import tqdm



def cric_betfair_wrangle(filenames,filename_to_save='betfair_data'):

    market_paths=[]
    for filename in filenames:
        market_paths.append('./Cricket Data/'+filename+'.tar')

    # the path directories to the data sets
    # accepts tar files, zipped files or 
    # directory with bz2 file(s)    
    
#---------------------------------------loading from tar and extracting bz2 files------------------------------
    
    def load_markets(file_paths: List[str]):
        for file_path in file_paths:
            if os.path.isdir(file_path):
                for path in glob.iglob(file_path + '**/**/*.bz2', recursive=True):
                    f = bz2.BZ2File(path, 'rb')
                    yield f
                    f.close()
            elif os.path.isfile(file_path):
                ext = os.path.splitext(file_path)[1]
                # iterate through a tar archive
                if ext == '.tar':
                    with tarfile.TarFile(file_path) as archive:
                        for file in archive:
                            yield bz2.open(archive.extractfile(file))
                # or a zip archive
                elif ext == '.zip':
                    with zipfile.ZipFile(file_path) as archive:
                        for file in archive.namelist():
                            yield bz2.open(archive.open(file))
        return None

#------------------------------------------betfairlightweight json interpretation---------------------------
          
    # setup logging
    logging.basicConfig(level=logging.INFO)

    # create trading instance (don't need username/password)
    trading = betfairlightweight.APIClient("username", "password")

    # create listener
    listener = StreamListener(max_latency=None)

    # create historical stream (update file_path to your file location)
    
    market_id=[]
    date=[]
    team_a=[]
    team_b=[]
    team_a_sp=[]
    team_b_sp=[]
    
    n_files=0
    for file in load_markets(market_paths):
        n_files+=1

    for file_obj in tqdm(load_markets(market_paths),total=n_files):

        stream = trading.streaming.create_historical_generator_stream(
            file_path=file_obj,
            listener=listener,
        )

        # create generator
        with patch("builtins.open", lambda f, _: f):
            gen = stream.get_generator()

            for market_books in gen():
                for market_book in market_books:
                        
                    team=[]
                    team_sp=[]
                    if market_book.inplay==False:
                        date_temp=market_book.publish_time
                        market_id_temp=market_book.market_id
                        for runner in market_book.runners:

                            team.append(next(rd.name for rd in market_book.market_definition.runners if rd.selection_id == runner.selection_id))

                            team_sp.append(runner.last_price_traded or np.NaN)

                        if len(market_book.runners)==2:

                            market_id.append(market_id_temp)
                            date.append(date_temp)
                            team_a.append(team[0])
                            team_b.append(team[1])
                            team_a_sp.append(team_sp[0])
                            team_b_sp.append(team_sp[1])

    data=pd.DataFrame.from_dict({'market_id':market_id,'date':date,'team_a':team_a,'team_b':team_b,
                                 'team_a_sp':team_a_sp,'team_b_sp':team_b_sp}).dropna()
    
    data.to_csv('./Cricket Data/'+filename_to_save+'.csv',index=False)
    
#------------------------------------------backtest wrangling-----------------------------------------------

    last_preplay_index=[]
    for id_loop in tqdm(data['market_id'].unique()):
        match_data=data[data['market_id']==id_loop]
        last_preplay_index.append(match_data.index[match_data['date'].argmax()])
        
    data=data[data.index.isin(last_preplay_index)].drop('market_id',axis=1).reset_index(drop=True)
    data['date']=data['date'].dt.date
    
    data.to_csv('./Cricket Data/'+filename_to_save+'.csv',index=False)

    return data