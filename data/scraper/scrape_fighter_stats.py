import pandas as pd
from sqlalchemy import create_engine
from lib import Scraper

conn_str = "mssql+pyodbc://localhost/ufc?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes&TrustServerCertificate=yes"
engine = create_engine(conn_str)

scraper = Scraper()

fighter_stats = scraper.scrape_fighter_details()
fighter_stats.to_sql('fighter_stats', con=engine, if_exists='replace', index=False)
print('Table (fighter_stats) created successfully!')

fight_stats = scraper.scrape_fight_details()
fight_stats.to_sql('fight_stats', con=engine, if_exists='replace', index=False)
print("Table (fight_stats) created successfully!")

scraper.close()

# fighter_stats.to_csv('C:/Users/Ben/Documents/ufc-fight-prediction/data/fighter_stats.csv')
# fight_stats.to_csv('C:/Users/Ben/Documents/ufc-fight-prediction/data/fight_stats.csv')


