import pandas as pd
import requests
from bs4 import BeautifulSoup
from typing import List
from tqdm import tqdm

class Scraper:
    def __init__(self):
        self.session = requests.Session()

    def get_soup(self, url: str) -> BeautifulSoup:
        '''
        Creates the BeautifulSoup object for the fighter at the given url
        
        :param url: url of the fighter stats
        :type url: str
        :return: BeautifulSoup object
        :rtype: BeautifulSoup
        '''
        page = self.session.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')

        # return
        return soup

    def get_urls(self, soup: BeautifulSoup) -> List[str]:
        '''
        Docstring for get_urls
        
        :param soup: BeautifulSoup object
        :type soup: BeautifulSoup
        :return: List of urls
        :rtype: List[str]
        '''
        rows = soup.find_all('tr', class_='b-statistics__table-row')[2:]
        urls = []
        for row in rows:
            urls.append(row.find('a', class_='b-link b-link_style_black')['href'])
        return urls


    def parse_fighter_details(self, soup: BeautifulSoup) -> List[str]:
        '''
        Parses the fighter details returning a list with the details:
        first name, last name, nickname, height, weight, reach, stance, wins, losses, draws, url
        
        :param soup: Soup object of the fighter
        :type soup: BeautifulSoup
        :return: List of details
        :rtype: List[str]
        '''


        li = soup.find_all('li', class_='b-list__box-list-item b-list__box-list-item_type_block')
        li = li[:9] + li[10:]

        row = []
        
        row.append(soup.find('span', class_='b-content__title-highlight').get_text(strip=True))
        row += soup.find('span', class_='b-content__title-record').get_text(strip=True).replace('Record: ', '').split('-')
        
        for item in li:
            text = item.get_text(strip=True)
            sep = text.split(':')
            row.append(sep[1])

        return row

    def scrape_fighter_details(self) -> pd.DataFrame:
        '''
        Docstring for scrape_fighter_details
        
        :param self: Description
        :return: Description
        :rtype: DataFrame
        '''
        chars = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
                'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
        col_labels = ['name', 'wins', 'losses', 'draws', 'height', 'weight', 'reach', 'stance', 'dob', 'sig_str_pm', 'str_acc', 'strikes_abs_pm', 'sig_str_def', 'td_avg', 'td_acc', 'td_def', 'sub_avg']
        df = pd.DataFrame(columns=col_labels)

        url = 'http://ufcstats.com/statistics/fighters'
        rows = []
        row = []
        for char in tqdm(chars, desc='Progress'):
            soup = self.get_soup(f'{url}?char={char}&page=all')
            for fighter_url in tqdm(self.get_urls(soup), desc=f'Fighters {char}/z', leave=False):
                fighter_soup = self.get_soup(fighter_url)
                row = self.parse_fighter_details(fighter_soup)
                rows.append(row)

        df = pd.DataFrame(rows, columns=col_labels)

        return df
    
    def parse_fight_details(self, soup: BeautifulSoup) -> List[str]:
        '''
        Docstring for parse_fight_details
        
        :param self: Description
        :param soup: Description
        :type soup: BeautifulSoup
        :return: Description
        :rtype: List[str]
        '''
        row = []
        tables = soup.find_all('table')[::2]

        row.append(soup.find('i', class_='b-fight-details__fight-title').get_text(strip=True)) # title
        row.append(soup.find('i', class_='b-fight-details__text-item_first').find_all('i')[1].get_text(strip=True)) # method
        fight_details_text = soup.find_all('i', class_='b-fight-details__text-item')

        row += map(lambda x: x.get_text(strip=True).split(':')[1], fight_details_text[:4]) # round, time, time_format, ref
        row.append(soup.find_all('p', class_='b-fight-details__text')[1].get_text(strip=True).replace('Details:', '')) # details

        # Totals table
        if len(tables) > 1:
            for col in tables[0].find('tbody').find_all('td'):
                p = col.find_all('p', class_='b-fight-details__table-text')
                row.append(p[0].get_text(strip=True))
                row.append(p[1].get_text(strip=True))

            # Significant strikes table
            for col in tables[1].find('tbody').find_all('td', class_='b-fight-details__table-col')[3:]:
                p = col.find_all('p', class_='b-fight-details__table-text')
                row.append(p[0].get_text(strip=True))
                row.append(p[1].get_text(strip=True))

            return row

    def scrape_fight_details(self, lim: int = None):
        soup = self.get_soup('http://ufcstats.com/statistics/events/completed?page=all')
        rows = []
        row = []

        col_labels = ['title', 'method', 'round', 'time', 'time_format', 'ref', 'details',
                      'blue_fighter', 'red_fighter', 'blue_fighter_kd', 'red_fighter_kd', 'blue_fighter_sig_str', 'red_fighter_sig_str',
                      'blue_fighter_sig_str_perc', 'red_fighter_sig_str_perc', 'blue_fighter_total_str', 'red_fighter_total_str',
                      'blue_fighter_td', 'red_fighter_td', 'blue_fighter_td_perc', 'red_fighter_td_perc', 'blue_fighter_sub_att', 'red_fighter_sub_att',
                      'blue_fighter_rev', 'red_fighter_rev', 'blue_fighter_ctrl', 'red_fighter_ctrl', 'blue_fighter_sig_str_head', 
                      'red_fighter_sig_str_head', 'blue_fighter_sig_str_body', 'red_fighter_sig_str_body', 'blue_fighter_sig_str_leg',
                      'red_fighter_sig_str_leg', 'blue_fighter_sig_str_distance', 'red_fighter_sig_str_distance', 'blue_fighter_sig_str_clinch',
                      'red_fighter_sig_str_clinch', 'blue_fighter_sig_str_ground', 'red_fighter_sig_str_ground']

        events = soup.find_all('tr', class_='b-statistics__table-row')[3:3 + lim] if lim else soup.find_all('tr', class_='b-statistics__table-row')[3:]

        for event in tqdm(events, desc='Event'):
            url = event.find('a', class_='b-link b-link_style_black')['href']
            event_soup = self.get_soup(url)
            for fight in event_soup.find_all('tr', class_='b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click'):
                fight_soup = self.get_soup(fight['data-link'])
                row = self.parse_fight_details(fight_soup)
                if row:
                    rows.append(row)

        df = pd.DataFrame(rows, columns=col_labels)

        return df

    def close(self):
        self.session.close()

if __name__ == '__main__':
    scraper = Scraper()
    # soup = scraper.get_soup('http://ufcstats.com/fighter-details/ce783bf73b5131f9')
    # data = scraper.parse_fighter_details(soup)
    data = scraper.scrape_fighter_details()
    scraper.close()

    print(data.head())
    # print(len(data))