import pandas as pd
import numpy as np
import re
import config
from sqlalchemy import create_engine

# Import data
fight_stats = pd.read_csv(config.PATH + '/ufc-stats/data/scraped_data/fight_stats_raw.csv')

def get_perc(x: str) -> float:
    '''
    Args:
    x (string): text in the format <landed> of <total>

    Returns:
    float: percentage (0 to 1) landed / total 
    '''
    if not isinstance(x, str):
        return np.nan
    try:
        y = x.split(' of ')
        if len(y) == 2:
            landed, total = int(y[0]), int(y[1])
            if total > 0:
                return round(landed / total, 2)
            return 0
    except (ValueError, TypeError):
        return np.nan

def get_attempts(x: str) -> int:
    '''
    Args:
    x (string): text in the format <landed> of <attempts>

    Returns:
    int: attempts
    '''
    if not isinstance(x, str):
        return np.nan
    try:
        return int(x.split(' of ')[1])
    except (ValueError, AttributeError, TypeError):
        return np.nan

def time_format_to_rounds(x: str) -> int:
    '''
    Args:
    x (string): time format of the fight

    Returns:
    int: maximum number of rounds in the fight
    '''
    if not isinstance(x, str):
        return np.nan    
    if x == 'No Time Limit':
        return 5
    try:
        return int(x[0])
    except (ValueError):
        return np.nan

def mins_per_round(x: str) -> int:
    '''
    Args:
    x (string): time format

    Returns:
    int: time limit (mins) per round in the fight
    '''
    if not isinstance(x, str):
        return np.nan
    try:
        mins = re.findall(r'\(([\d\-]*)\)', x)
        if len(mins):
            mins = mins[0].split('-')
            if len(set(mins)) == 1:
                return int(mins[0])
        return np.nan
    except (ValueError, TypeError):
        return np.nan

# Columns with the format '<landed> of <attempts>'
cols = ['blue_fighter_sig_str', 'red_fighter_sig_str',
        'blue_fighter_total_str', 'red_fighter_total_str',
        'blue_fighter_td', 'red_fighter_td',
        'blue_fighter_sig_str_head', 'red_fighter_sig_str_head',
        'blue_fighter_sig_str_body', 'red_fighter_sig_str_body',
        'blue_fighter_sig_str_leg', 'red_fighter_sig_str_leg',
        'blue_fighter_sig_str_distance', 'red_fighter_sig_str_distance',
        'blue_fighter_sig_str_clinch', 'red_fighter_sig_str_clinch',
        'blue_fighter_sig_str_ground', 'red_fighter_sig_str_ground']

for col in cols:
    fight_stats[col] = fight_stats[col].fillna('0 of 0') # Fill null values
    fight_stats[col + '_perc'] = fight_stats[col].map(lambda x: get_perc(x)) # percentage column
    fight_stats[col + '_att'] = fight_stats[col].map(lambda x: get_attempts(x)) # attempts column
        
# Drop columns
fight_stats = fight_stats.drop(columns=cols)

# Convert date column to datetime
fight_stats.date = fight_stats.date.map(lambda x: pd.to_datetime(x, format='%B %d, %Y'))

# Column containing maximum number of rounds in the fight
fight_stats['fight_num_rounds'] = fight_stats.time_format.map(lambda x: time_format_to_rounds(x))

# Column containing the time limit of each round 
fight_stats['round_mins'] = fight_stats.time_format.map(lambda x: mins_per_round(x))

# Fills null values in round_mins with the most common round time limit
fight_stats = fight_stats.fillna({'round_mins': fight_stats['round_mins'].mode()[0]})

# Drop time_format column
fight_stats = fight_stats.drop(columns='time_format')

def get_score_dif(x: str) -> int:
    '''
    Args:
    x (string): text containing fight details

    Returns:
    int: difference between the blue fighter and red fighters scores
    '''
    if not isinstance(x, str):
        return np.nan
    try:
        scores = re.findall('[0-9]{2}', x)
        if len(scores) == 6:
            blue_total = int(scores[0]) + int(scores[2]) + int(scores[4])
            red_total = int(scores[1]) + int(scores[3]) + int(scores[5])
            return blue_total - red_total
        return np.nan
    except (ValueError, TypeError):
        return np.nan

# Create outcome column with 'red' if red wins, 'blue' if blue wins, 'draw' if the fight is a draw and 'nc' if no contest
fight_stats['outcome'] = fight_stats.red_outcome.replace({'W': 'red', 'L': 'blue', 'D': 'draw', 'NC': 'nc'})

# Drop red_outcome and blue_outcome
fight_stats.drop(columns=['red_outcome', 'blue_outcome'], inplace=True)

# Score difference column
fight_stats['score_dif'] = fight_stats.details.map(lambda x: get_score_dif(x)) * fight_stats.outcome.replace({'red': 1, 'blue': 1, 'draw': 0, 'nc': 0})

fight_stats['title'] = fight_stats['title'].str.lower()
fight_stats['rtufc'] = fight_stats.title.map(lambda x: 1 if re.search(r'^road *to *ufc', x) else 0)
fight_stats['gender'] = fight_stats.title.map(lambda x: 0 if re.search(r"women's", x) else 1)
fight_stats['tournament'] = fight_stats.title.map(lambda x: 1 if re.search(r'tournament', x) else 0)
fight_stats['title_bout'] = fight_stats.title.map(lambda x: 1 if re.search(r'title *bout', x) else 0)
fight_stats['interim_title_bout'] = fight_stats.title.map(lambda x: 1 if re.search(r'interim *title *bout', x) else 0)

# The following columns are null for fights between 1994 and 1998
# Replace null values with zeros
fill_na = {
    'red_fighter_kd': 0.0,
    'blue_fighter_kd': 0.0,
    'red_fighter_sub_att': 0,
    'blue_fighter_sub_att': 0,
    'red_fighter_rev': 0,
    'blue_fighter_rev': 0,
    'red_fighter_ctrl': 0,
    'blue_fighter_ctrl': 0
}
fight_stats = fight_stats.fillna(fill_na)

fight_stats.to_csv(config.PATH + '/ufc-stats/data/clean_data/fight_stats_clean.csv')

conn_str = f"mssql+pyodbc://{config.DB_SERVER}/{config.DB_NAME}?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes&TrustServerCertificate=yes"
engine = create_engine(conn_str)

fight_stats.to_sql('fight_stats', con=engine, if_exists='replace', index=False)
print("Table (fight_stats) created successfully!")