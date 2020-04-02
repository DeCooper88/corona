import requests as rq
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep


BASE_WIKI_URL = "https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_"
cs = 'Austria Belgium Denmark France Germany Italy the_Netherlands Spain the_United_Kingdom the_United_States'
cc = 'at be dk fr de it nl es uk us'
countries = {country: code for country, code in zip(cs.split(), cc.split())}


def get_wiki_page(country):
    """download corona wiki page for country and return as BeautifulSoup object"""
    url = BASE_WIKI_URL + country
    wiki_page = rq.get(url)
    wiki_page.raise_for_status()
    return BeautifulSoup(wiki_page.text, 'html.parser')


def get_number(data_string):
    if len(data_string) == 0:
        return 0
    elif '(' in data_string:
        n, *_ = data_string.split('(')
        number = n.replace(',', '')
        return int(number)
    else:
        number = data_string.replace(',', '')
        return int(number)


def correct(date):
    d, m, y = date.split('-')
    return '-'.join([y, m, d])


def clean_data(row):
    date, _, c, d = row
    if date.endswith('2020'):
        date = correct(date)
    cases = get_number(c.strip())
    deaths = get_number(d.strip())
    return date, cases, deaths


def get_table_rows(country):
    soup = get_wiki_page(country)
    # get the table
    table = soup.find('table', {'style': "text-align:left; border-collapse:collapse; width:100%;"})
    rows = table.find_all('tr')
    table_data = []
    for row in rows[2:-1]:
        row_strings = [col.text for col in row.find_all('td')]
        if row_strings[0] != '⋮':
            table_data.append(clean_data(row_strings))
    return table_data


def create_df(country):
    "create dataframe from country data"
    data = get_table_rows(country)
    cols = ['date', 'cases_' + countries[country], 'deaths_' + countries[country]]
    df = pd.DataFrame(data, columns=cols)
    df.date = pd.to_datetime(df.date)
    last_date, _ = str(df.iloc[-1, 0]).split(' ')
    print(f'Data upto {last_date} collected for {country}.')
    return df


def fill_missing_data(df):
    # fix top row
    df.iloc[0, :] = df.iloc[0, :].fillna(0)
    # fill missing values
    df = df.fillna(method='ffill')
    df.iloc[:, 1:] = df.iloc[:, 1:].astype(int)
    return df


def get_data(countries):
    today = pd.to_datetime('today')
    yesterday = today - pd.DateOffset(days=1)
    # start date is when first case was reported in US
    dates = pd.date_range(start='01-21-2020', end=yesterday)
    df = pd.DataFrame(dates, columns=['date'])
    print('Base dataframe created')
    for country in countries:
        country_data = create_df(country)
        # pause to spread load over host website
        sleep(5)
        df = df.merge(country_data, how='left', on='date')
    print('Fill missing data.')
    df = fill_missing_data(df)
    print('Dataframe ready.')
    return df


# req_countries = ['Italy', 'Spain', 'the_United_States', 'France', 'the_United_Kingdom',
#                  'the_Netherlands', 'Germany', 'Belgium', 'Denmark', 'Austria']

test_countries = ['France', 'the_Netherlands', 'Belgium']

wiki_data = get_data(test_countries)

print(wiki_data.head())
print()
print(wiki_data.info())
