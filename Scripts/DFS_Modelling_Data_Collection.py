import itertools
import random
import difflib
import os
import pandas as pd
import numpy as np
import scipy.stats as ss
import requests
import json
import collections
import warnings
from pandas.errors import SettingWithCopyWarning

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
#create funcs to get finishing positions
def pos_to_num(finish_pos):
    if finish_pos in ['n/a', 'WD']:
        return(None)
    elif 'T' in finish_pos:
        return(int(finish_pos.replace('T', '')))
    else:
        return(int(finish_pos))

#get api key
path = '/Users/Broth/Personal_Projects/Golf_DFS/Data/'
file = open(path+'key.txt')
key = file.read().replace("\n", " ")
file.close()

#define api options
file_format = 'json'
event_id_url = "https://feeds.datagolf.com/historical-raw-data/event-list?file_format={}&key={}".format(file_format, key)

#read results
html_events = requests.get(event_id_url).text

#convert to dict, filter to relevant tours
tours= ['pga', 'kft', 'liv', 'euro']
html_dict_events = json.loads(html_events)
html_events_filt = [event for event in html_dict_events if event['tour'] in tours]
#remove zurirch
html_events_filt = [event for event in html_dict_events if event['event_name'] != 'Zurich Classic of New Orleans']

events_df = pd.DataFrame()

#fin_txts = [html_dict_results['scores'][i]['fin_text'] for i in range(len(html_dict_results['scores']))]
#fin_txts = [fin for fin in fin_txts if fin not in ['WD', 'CUT']]
#fin_txts = [int(fin.replace('T', '')) if 'T' in fin else int(fin) for fin in fin_txts]

curr_event = 0
total_event_count = len(html_events_filt)

for html_event in html_events_filt:

    #get api inputs
    tour = html_event['tour']
    date = html_event['date']
    event_id = html_event['event_id']
    event_name = html_event['event_name']
    year = html_event['calendar_year']

    #get results for each tournament
    results_url = "https://feeds.datagolf.com/historical-raw-data/rounds?tour={}&event_id={}&year={}&file_format={}&key={}".\
        format(tour, event_id, year, file_format, key)

    #read results
    html_results = requests.get(results_url).text

    #convert to dict
    html_dict_results = json.loads(html_results)


    event_id = html_dict_results['event_id']
    tour = html_dict_results['tour']
    year = html_dict_results['year']

    event_df = pd.DataFrame()

    for i in range(len(html_dict_results['scores'])):
        player_dct = html_dict_results['scores'][i]

        dg_id = player_dct['dg_id']
        player_name = player_dct['player_name']
        finish_pos = player_dct['fin_text']

        finish_pos = np.where(finish_pos == 'n/a', 0, finish_pos)

        player_results_df = pd.DataFrame()

        round_keys = [key for key in player_dct.keys() if key[:5] == 'round']

        for round in round_keys:
            player_round_df = pd.DataFrame([player_dct[round]])
            player_round_df['round'] = round
            player_results_df = pd.concat([player_results_df, player_round_df], ignore_index = True)

        player_results_df['score_to_par'] = player_results_df.score - player_results_df.course_par
        player_results_df['dg_id'] = dg_id
        player_results_df['player_name'] = player_name


        event_df = pd.concat([event_df, player_results_df], ignore_index = True)

    #define id cols
    event_df['event_id'] = html_dict_results['event_id']
    event_df['tour'] = html_dict_results['tour']
    event_df['year'] = html_dict_results['year']
    event_df['season'] = html_dict_results['season']
    event_df['event_name'] = event_name

    #get finish pos by player/round
    event_ranks_df = pd.DataFrame()
    for round in event_df['round'].unique():
        event_filt_df = event_df.loc[event_df['round'] == round, ]
        event_filt_df.sort_values('score', ascending = True).reset_index(inplace = True)
        event_filt_df.loc[:,'round_finish'] = ss.rankdata(event_filt_df.score, method = 'min')
        event_ranks_df = pd.concat([event_ranks_df, event_filt_df], ignore_index = True)

    #merge in round finish
    event_df = pd.merge(event_df, event_ranks_df[['player_name', 'round', 'round_finish']], on = ['player_name', 'round'] ) 

    #merge in event
    events_df = pd.concat([events_df, event_df], ignore_index = True)

    #add one to event count
    curr_event += 1

    #track process
    print('Event ' + str(curr_event) + ' of ' + str(total_event_count) + ' completed.')


events_df.to_csv(path + 'historical_events.csv', index=False)