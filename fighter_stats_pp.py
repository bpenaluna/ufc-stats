import pandas as pd
import numpy as np
import re
import config
from sqlalchemy import create_engine

# Import data
fighter_stats = pd.read_csv(config.PATH + '/ufc-stats/data/scraped_data/fighter_stats_raw.csv')

# There are many instances of '--', replace these with null values
fighter_stats = fighter_stats.replace('--', np.nan)

# Remove symbols in numerical columns and convert column to float type
fighter_stats.weight = fighter_stats.weight.str.replace(' lbs.', '').astype(np.float64)
fighter_stats.reach = fighter_stats.reach.str.replace('"', '').astype(np.float64)
fighter_stats.str_acc = fighter_stats.str_acc.str.replace('%', '').astype(np.float64)
fighter_stats.sig_str_def = fighter_stats.sig_str_def.str.replace('%', '').astype(np.float64)
fighter_stats.td_acc = fighter_stats.td_acc.str.replace('%', '').astype(np.float64)
fighter_stats.td_def = fighter_stats.td_def.str.replace('%', '').astype(np.float64)

# dob column to datetime
fighter_stats.dob = fighter_stats.dob.map(lambda x: pd.to_datetime(x, format='%b %d, %Y'))

def inches_from_feet(x):
    '''
    Args:
    x (string): feet and inches

    Returns:
    int: inches
    '''
    if pd.isna(x):
        return np.nan
    if not isinstance(x, str):
        return np.nan
    try:
        y = x.split('\'')
        feet, inches = int(y[0]), int(y[1][:-1])
        return int(feet * 11 + inches)
    except (ValueError, TypeError):
        return np.nan

# Convert height column to inches
fighter_stats.height = fighter_stats.height.map(lambda x: inches_from_feet(str(x)))

# Convert numerical columns to numerical types
fighter_stats.wins = fighter_stats.wins.astype(int)
fighter_stats.losses = fighter_stats.losses.astype(int)
fighter_stats.sig_str_pm = fighter_stats.sig_str_pm.astype(np.float64)
fighter_stats.strikes_abs_pm = fighter_stats.strikes_abs_pm.astype(np.float64)
fighter_stats.td_avg = fighter_stats.td_avg.astype(np.float64)
fighter_stats.sub_avg = fighter_stats.sub_avg.astype(np.float64)

def sum_draws(x: str) -> int:
    '''
    Args:
    x (string): number of draws and no contests

    Returns:
    int: sum of draws and no contests
    '''
    pattern = pattern = '\((\d+) NC\)'
    if isinstance(x, str):
        match = re.search(pattern, x)
        if match:
            return int(match.group(1))
        return int(x)
    return np.nan

# Sums draws and no contests in the draws column
fighter_stats.draws = fighter_stats.draws.map(lambda x: sum_draws(x))

# Fill null values
fill_na = {
    'height': fighter_stats.height.mean(),
    'reach': fighter_stats.reach.mean(),
    'weight': fighter_stats.weight.mean(),
    'dob': fighter_stats.dob.median(),
    'nickname': '',
    'stance': fighter_stats.stance.mode()[0]
}
fighter_stats = fighter_stats.fillna(fill_na)

# Total fights column
fighter_stats['total_fights'] = fighter_stats.wins + fighter_stats.losses + fighter_stats.draws

# Fighter birth year, month and day columns
fighter_stats['birth_year'] = fighter_stats.dob.map(lambda x: x.year)
fighter_stats['birth_month'] = fighter_stats.dob.map(lambda x: x.month)
fighter_stats['birth_day'] = fighter_stats.dob.map(lambda x: x.day)

# Save clean fighter_stats dataframe as fighter_stats_clean.csv
fighter_stats.to_csv(config.PATH + '/ufc-stats/data/clean_data/fighter_stats_clean.csv')

# Create connection to database
conn_str = f"mssql+pyodbc://{config.DB_SERVER}/{config.DB_NAME}?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes&TrustServerCertificate=yes"
engine = create_engine(conn_str)

# Save clean data to database
fighter_stats.to_sql('fighter_stats', con=engine, if_exists='replace', index=False)
print('Table (fighter_stats) created successfully!')