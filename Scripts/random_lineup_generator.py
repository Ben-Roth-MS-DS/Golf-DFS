import itertools
import random
import time
import difflib
import os
import pandas as pd
import numpy as np
import requests
import json
import collections

#define api options
tour = 'pga'
odds_format = 'percent'
file_format = 'json'

#get api key
key_path = '/Users/Broth/Personal_Projects/Golf_DFS/Data/'
file = open(key_path+'key.txt')
key = file.read().replace("\n", " ")
file.close()

#create url endpoint
url = 'https://feeds.datagolf.com/preds/pre-tournament?tour={}&odds_format={}&file_format={}&key={}'.format(tour, odds_format, file_format, key)

#read results
html = requests.get(url).text

#convert to dict
html_dict = json.loads(html)

#convert to df
df_hist = pd.DataFrame(html_dict['baseline_history_fit'])
df_base = pd.DataFrame(html_dict['baseline'])

#merge together base and baseline_fit_history models
df_base_hist = pd.merge(df_hist, df_base, on = 'player_name', suffixes=['', '_base'])

#get first name last name
df_base_hist['first_name'] = df_base_hist['player_name'].apply(lambda x: x.split(', ')[1])
df_base_hist['last_name'] = df_base_hist['player_name'].apply(lambda x: x.split(', ')[0])
df_base_hist['first_last_name'] = df_base_hist['first_name'] + ' ' + df_base_hist['last_name'] 

#get avg make cut and top 20 
odd_cols = ['make_cut', 'top_5', 'top_10', 'top_20']
for col in odd_cols:
    base_col = col + '_base'
    df_base_hist['avg_' + col] = df_base_hist[[col, base_col]].mean(axis=1)



all_cols = odd_cols + [col + '_base' for col in odd_cols]
avg_cols = ['avg_' + col for col in odd_cols]

df_base_hist['avg_all'] = df_base_hist[all_cols].mean(axis=1)

#read in dk salaries csv
path = '/Users/Broth/Downloads/DKSalaries.csv'
columns = ['Golfer1', 'Golfer2', 'Golfer3', 'Golfer4', 'Golfer5', 'Golfer6', 'Empty', 'Position','Name + ID', 'Name','ID', 'Roster Position', 'Salary', 'Game Info', 'TeamAbbrev', 'AvgPointsPerGame']
salaries_df = pd.read_csv(path, names=columns)

#merge salaries with datagolf odds
comb_df = pd.merge(salaries_df, df_base_hist[['first_last_name'] + all_cols + avg_cols + ['avg_all']], left_on = 'Name', right_on = 'first_last_name', how = 'inner')

comb_df.Salary = comb_df.Salary.astype(int)

#filter down list of players to not break computer
comb_df = comb_df.loc[comb_df.avg_top_20 > 0.125,].reset_index(drop = True)

def smooth_top_5(column):
    max_value = 0.6
    min_value = 0.01

    # Calculate the square root of the values
    sqrt_values = np.sqrt(column)
    
    # Calculate the scaling factor based on the square root of the max and min values
    scale_factor = np.sqrt(max_value) / np.max(sqrt_values)
    
    # Apply the scaling factor to the square root of each value in the column
    smoothed_values = sqrt_values * scale_factor
    
    # Square the smoothed values to revert to the original scale
    smoothed_values = smoothed_values ** 2
    
    # Clip values to ensure they do not exceed the original max value
    smoothed_values = np.clip(smoothed_values, None, max(column))
    
    # Clip values to ensure they do not go below the desired min value
    smoothed_values = np.maximum(smoothed_values, min_value)
    
    return smoothed_values


#smooth column
for col in avg_cols + ['avg_all']:
    smooth_col = col.replace('avg', 'smoothed')
    comb_df[smooth_col] = smooth_top_5(comb_df[col])

#define player dictionary
players_dct = {comb_df.loc[i, 'Name']:comb_df.loc[i, 'Salary'] for i in range(len(comb_df))}
pct_dct = {comb_df.loc[i, 'Name']:comb_df.loc[i, 'avg_top_10'] for i in range(len(comb_df))}

#define max team salary and number of players in lineup
max_val = 50000
max_num = 6

#create list of names and values
players_lst = [name for name in players_dct.keys()]
salaries_lst = [players_dct[name] for name in players_lst]
pct_lst = [pct_dct[name] for name in pct_dct.keys()]

#create nested list with all possible lineup combinations
all_combos = itertools.combinations(players_lst, 6)
all_combos = [list(combo) for combo in all_combos]

#break combos into batches so memory doesn't die
def batching(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


#all_combos = [[(name, players_dct[name], pct_dct[name]) for name in l] for l in all_combos]
new_combos = []

#track progress
i = 0
for batch in batching(all_combos, n=1000000):
    new_combo = [[(name, players_dct[name], pct_dct[name]) for name in lineup] for lineup in batch]
    new_combos += new_combo
    i += 1
    print(i)



#define function that creates all possible legal combinations of players within legal parameters
def comb_returner(combination, max_value = 50000, min_value = 49500):
    price = [int(tup[1]) for tup in combination]
    if sum(price) <= max_value and sum(price) >= min_value:
        return(combination)


filtered_combos = [comb_returner(combo, max_value=max_val) for combo in new_combos if comb_returner(combo, max_value=max_val, min_value=49500) is not None]

del(new_combos)
del(all_combos)

def comb_orderer(combos):
    combo_sums = []
    for combo in combos:
        make_cut_total = sum([val[2] for val in combo])
        combo_sum = combo + [make_cut_total]
        combo_sums.append(combo_sum)

    ordered_combos = sorted(combo_sums, key = lambda x: x[6], reverse = True)

    return(ordered_combos)

ordered_comb = comb_orderer(filtered_combos)

del(filtered_combos)

#get list of player names to act as counters
field = {player:0 for player in comb_df.Name.values}
temp_field = {player:0 for player in comb_df.Name.values}

top20 = []

i = 0
sixkcounter = 0

for n in range(len(ordered_comb)):
    i += 1
    #get list of names
    pot_lineup = [player[0] for player in ordered_comb[n][:6]]

    #get list of potential salaries
    pot_sal = [salary[1] for salary in ordered_comb[n][:6]]

    #add one to temp counter
    for player in pot_lineup:
        temp_field[player] += 1
    
    #check if any players are represented at a higher rate than their make cut %
    if any((field[player])/20 > pct_dct[player] for player in pot_lineup):
        for player in pot_lineup:
            temp_field[player] -= 1
        pass

    elif sixkcounter > 10 and sum(1 for player in pot_lineup if int(players_dct[player]) < 7000) > 1:
        pass

    #if passes both checks
    else:
        for player in pot_lineup:
            field[player] += 1

        name_counter = sum(1 for player in pot_lineup if int(players_dct[player]) < 7000)
        if name_counter > 0:
            sixkcounter += 1



        top20.append(ordered_comb[n][:6])
    print(str(len(top20)) + ' lineups added from ' + str(i) + ' potential linesup')
    if len(top20) < 20:
        continue
    else:
        break

merged = list(itertools.chain(*top20))
merged = [tup[0] for tup in merged]
collections.Counter(merged)



def id_getting(player, df, name_column, id_column):
    #function that gets player id from matching exercise
    name = difflib.get_close_matches(player, df[name_column].dropna().values, n = 1)[0]
    id = df.loc[df[name_column] == name, id_column].values[0]
    return(id)

player_ids = [[id_getting(player[0], salaries_df, 'Name', 'ID') for player in lineup] for lineup in top20]

salaries_df.loc[range(1, len(player_ids) + 1), ['Golfer'+str(i) for i in range(1,7)]] = player_ids

salaries_df.to_csv(path.replace('DKSalaries', 'DKSalaries_Updated'), index=False, header=False)

os.remove(path)
x = 0

for n in range(len(ordered_comb)):
    x += 1
    #get list of names
    pot_lineup = [player[0] for player in ordered_comb[n][:6]]

    print('Checking lineup ' + str(x) + ' of ' + str(len(ordered_comb)) + ' for Justin Thomas.')

    if 'Jordan Spieth' in pot_lineup:
        print('Justin Thomas found in lineup ' + str(x) + '.')
        break
    else:
        continue