import pandas as pd
import numpy as np

from sklearn.model_selection import StratifiedKFold, RandomizedSearchCV, cross_val_score, train_test_split
from sklearn.metrics import mean_squared_error
from xgboost import XGBRegressor

#read in dataframes
path = '/Users/Broth/Personal_Projects/Golf_DFS/Data/'
events_df = pd.read_csv(path + 'historical_events.csv')
course_df = pd.read_csv(path + 'dg_course_values_table.csv')

#join together
joined_df = pd.merge(events_df, course_df, left_on = 'course_name', right_on = 'course')

#reveres order of df
joined_df = joined_df.iloc[::-1].reset_index(drop=True)

#calculate difference between course sgs and 
course_cats = ['ott_sg', 'app_sg', 'arg_sg', 'putt_sg', 'adj_driving_distance', 'adj_driving_accuracy', 'adj_gir']
event_cats = ['sg_ott', 'sg_app', 'sg_arg', 'sg_putt', 'driving_dist', 'driving_acc', 'gir']

for i in range(len(course_cats)):
    course_sg = course_cats[i]
    event_sg = event_cats[i]

    joined_df[event_sg + '_diff'] = joined_df[event_sg] - joined_df[course_sg]


id_cols = ['course_name', 'course_num', 'dg_id', 'player_name', 'event_id', 'tour', 'year', 'season', 'event_name']
agg_cols = [ 'driving_acc', 'driving_dist', 'gir', 'great_shots', 'prox_fw', 'prox_rgh', 'scrambling', 'round_finish', 'score', 'sg_total', \
            'score_to_par', 'sg_ott', 'sg_putt', 'sg_t2g', 'sg_app', 'sg_arg', 'poor_shots'] + [cat + '_diff' for cat in event_cats]

# Function to calculate rolling averages for different window sizes for specified columns
def rolling_averages(df, column_names, window_sizes=[10, 25, 50, 100]):
    result = pd.DataFrame()
    for window_size in window_sizes:
        for col in column_names:
            # Group by 'player_name' and calculate rolling mean for specified column
            rolling_avg = df.groupby('player_name')[col].transform(lambda x: x.rolling(window=window_size).mean())
            result[f'{col}_avg_last_{window_size}'] = rolling_avg
    return result

# Call the function to get rolling averages
rolling_averages_df = rolling_averages(joined_df, agg_cols)

# Concatenate original DataFrame with the DataFrame containing rolling averages
result_df = pd.concat([joined_df, rolling_averages_df], axis=1)

# Separate features and target variable
X = result_df[agg_cols + [col for col in result_df.columns if 'avg_last' in col]].drop(columns=['score'])
y = joined_df['score']

# Split the data into training and test sets, stratified by 'tour' and 'year'
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.33, stratify=joined_df[['tour', 'season']]
)



# Define inner and outer cross-validation strategies with stratification
inner_cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=1234)
outer_cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=1234)

# Define XGBoost regressor
xgb_reg = XGBRegressor()

# Define parameter distributions for randomized search
param_grid = {
    'learning_rate': [0.01, 0.05, 0.1, 0.25],
    'max_depth': [3, 5, 6],
    'min_child_weight': [1, 3, 5, 7],
    'gamma': [0.0, 0.1, 0.2 , 0.3, 0.4, 0.5, 0.6, 0.7],
    'colsample_bytree': [0.3, 0.4, 0.5 , 0.7],
    'subsample': [0.6, 0.7, 0.8]
}

# Perform nested cross-validation with randomized search
random_search = RandomizedSearchCV(estimator=xgb_reg,
                                    param_distributions=param_grid,
                                    n_iter=10, cv=inner_cv,
                                    scoring='neg_mean_squared_error',
                                    random_state=1234)

nested_score = cross_val_score(random_search, X=X_train, y=y_train, cv=outer_cv, scoring='neg_mean_squared_error')
