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
        name, height, weight, reach, stance, wins, losses, draws
        
        :param soup: Soup object of the fighter
        :type soup: BeautifulSoup
        :return: List of details
        :rtype: List[str]
        '''


        li = soup.find_all('li', class_='b-list__box-list-item b-list__box-list-item_type_block')
        li = li[:9] + li[10:]

        row = []
        
        name = soup.find('span', class_='b-content__title-highlight')
        row.append(name.get_text(strip=True) if name else '--')
        
        nickname = soup.find('p', class_='b-content__Nickname')
        row.append(nickname.get_text(strip=True) if nickname else '--')

        record = soup.find('span', class_='b-content__title-record')
        row += record.get_text(strip=True).replace('Record: ', '').split('-') if record else ['--', '--', '--']
        
        for item in li:
            text = item.get_text(strip=True)
            sep = text.split(':')
            row.append(sep[1])

        return row

    def scrape_fighter_details(self) -> pd.DataFrame:
        '''
        Scrapes fighter details from ufcstats.com
        
        :return: DataFrame containing fighter details
        :rtype: DataFrame
        '''
        chars = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
                'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
        col_labels = ['fighter_id', 'name', 'nickname', 'wins', 'losses', 'draws', 'height', 'weight', 'reach', 'stance', 'dob', 'sig_str_pm', 'str_acc', 'strikes_abs_pm', 'sig_str_def', 'td_avg', 'td_acc', 'td_def', 'sub_avg', 'url']
        df = pd.DataFrame(columns=col_labels)

        url = 'http://ufcstats.com/statistics/fighters'
        rows = []
        row = []
        fighter_id = 0
        for char in tqdm(chars, desc='Progress'):
            soup = self.get_soup(f'{url}?char={char}&page=all')
            for fighter_url in tqdm(self.get_urls(soup), desc=f'Fighters {char}/z', leave=False):
                fighter_soup = self.get_soup(fighter_url)
                row.append(fighter_id)
                row = self.parse_fighter_details(fighter_soup)
                row.append(fighter_url)
                rows.append(row)
                fighter_id += 1

        df = pd.DataFrame(rows, columns=col_labels)

        return df
    
    def parse_fight_details(self, soup: BeautifulSoup) -> List[str]:
        '''
        Parses the fight details returning a list.
        
        :type soup: BeautifulSoup
        :return: row containing the details from a single fight
        :rtype: List[str]
        '''

        row = []

        # title
        title = soup.find('i', class_='b-fight-details__fight-title')
        row.append(title.get_text(strip=True) if title else '--')

        # method
        method = soup.find('i', class_='b-fight-details__text-item_first').find_all('i')
        row.append(method[1].get_text(strip=True) if len(method) > 1 else '--')
        
        # round, time, time_format, ref
        fight_details_text = soup.find_all('i', class_='b-fight-details__text-item')
        for detail in fight_details_text[:4]:
            row.append(detail.get_text(strip=True).split(':')[1])

        # details
        details = soup.find_all('p', class_='b-fight-details__text')
        row.append(details[1].get_text(strip=True).replace('Details:', '') if details else '--')
        
        # fight outcomes and nicknames
        fight_details = soup.find_all('div', class_='b-fight-details__person')
        for detail in fight_details:
            row.append(detail.find('i').get_text(strip=True)) # red_outcome, blue_outcome
            row.append(detail.find('p', class_='b-fight-details__person-title').get_text(strip=True)) # red_nickname, blue_nickname
        
        tables = soup.find_all('table')[::2]

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

        links = soup.find_all('a', class_='b-link b-fight-details__person-link')
        for link in links:
            row.append(link['href'])

        return row

    def scrape_fight_details(self, lim: int = None):
        '''
        Scrapes fight details from ufcstats.com.
        
        :param lim: The number of events to scrape. Default (None) returns all events
        :type lim: int
        :return: DataFrame with data from ufc fights
        :rtype: DataFrame
        '''

        soup = self.get_soup('http://ufcstats.com/statistics/events/completed?page=all')
        rows = []
        row = []

        col_labels = ['date', 'location', 'title', 'method', 'round', 'time', 'time_format', 'ref', 'details', 'red_outcome', 'blue_outcome', 'red_nickname', 'blue_nickname', 
                      'red_fighter', 'blue_fighter', 'red_fighter_kd', 'blue_fighter_kd', 'red_fighter_sig_str', 'blue_fighter_sig_str',
                      'red_fighter_sig_str_perc', 'blue_fighter_sig_str_perc', 'red_fighter_total_str', 'blue_fighter_total_str',
                      'red_fighter_td', 'blue_fighter_td', 'red_fighter_td_perc', 'blue_fighter_td_perc', 'red_fighter_sub_att', 'blue_fighter_sub_att',
                      'red_fighter_rev', 'blue_fighter_rev', 'red_fighter_ctrl', 'blue_fighter_ctrl', 'red_fighter_sig_str_head', 
                      'blue_fighter_sig_str_head', 'red_fighter_sig_str_body', 'blue_fighter_sig_str_body', 'red_fighter_sig_str_leg',
                      'blue_fighter_sig_str_leg', 'red_fighter_sig_str_distance', 'blue_fighter_sig_str_distance', 'red_fighter_sig_str_clinch',
                      'blue_fighter_sig_str_clinch', 'red_fighter_sig_str_ground', 'blue_fighter_sig_str_ground', 'red_fighter_url', 'blue_fighter_url']

        events = soup.find_all('tr', class_='b-statistics__table-row')[2:2 + lim] if lim else soup.find_all('tr', class_='b-statistics__table-row')[2:]

        for event in tqdm(events, desc='Event'):
            date = event.find('span', class_='b-statistics__date').get_text(strip=True)
            location = event.find('td', class_='b-statistics__table-col b-statistics__table-col_style_big-top-padding').get_text(strip=True)
            url = event.find('a', class_='b-link b-link_style_black')['href']
            event_soup = self.get_soup(url)
            for fight in event_soup.find_all('tr', class_='b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click'):
                fight_soup = self.get_soup(fight['data-link'])
                fight_details = self.parse_fight_details(fight_soup)
                if fight_details:
                    row = [(date or '--'), (location or '--')] + self.parse_fight_details(fight_soup)
                    rows.append(row)

        df = pd.DataFrame(rows, columns=col_labels)

        return df

    def scrape_rankings(self):
        '''
        Scrapes rankings data from ufc.com.
        
        :return: DataFrame containing the current UFC rankings
        :rtype: DataFrame
        '''
        soup = self.get_soup('https://www.ufc.com/rankings')

        col_labels = ['fighter', 'ranking', 'weight_class']

        row = []
        rows = []

        for el in soup.find_all('div', class_='view-grouping-content'):
            weight_class = el.find('div', class_='info').find('h4').get_text(strip=True)
            for fighter in el.find_all('tbody').find_all('tr'):
                details = fighter.find_all('td')
                row += [details[0].get_text(strip=True), details[1].get_text(strip=True)]
                row.append(weight_class)
                rows.append(row)

        return pd.DataFrame(rows, columns=col_labels)

    def close(self):
        self.session.close()

if __name__ == '__main__':
    scraper = Scraper()
    # soup = scraper.get_soup('http://ufcstats.com/fighter-details/ce783bf73b5131f9')
    # data = scraper.parse_fighter_details(soup)
    data = scraper.scrape_rankings()
    scraper.close()

    print(data)
    # print(data.shape)