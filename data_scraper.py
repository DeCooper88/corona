from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
import requests
from time import sleep


cw = (
    "Austria Belgium Canada Denmark France Germany Italy Japan the_Netherlands "
    + "New_York_(state) South_Korea Spain Sweden the_United_Kingdom the_United_States"
)
cc = "at be ca dk fr de it jp nl ny kr es se uk us"

# dictionary that get country short-code for wikipedia country name
wiki_shortcodes = {country: code for country, code in zip(cw.split(), cc.split())}

# country_names is dictionary that gets country name for country short-code
# this ensures names are proper python variable names, suitable for dataframe columns
country_names = {code: country.lower() for country, code in wiki_shortcodes.items()}
country_names["nl"] = "netherlands"
country_names["ny"] = "ny_state"
country_names["uk"] = "uk"
country_names["us"] = "us"


def get_wiki_pages(countries, pause=3):
    """
    Download corona wikipedia pages for countries and return as BeautifulSoup object.
    countries(list): countries to download
    pause(int): number of seconds to pause between web requests
    """
    base_wiki_url = "https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_"
    pause_count = 0
    for country in countries:
        if pause_count > 0:
            sleep(pause)
        url = base_wiki_url + country
        wiki_page = requests.get(url)
        wiki_page.raise_for_status()
        yield BeautifulSoup(wiki_page.text, "html.parser")
        pause_count += 1


def get_number(data_string):
    """Extract number from data_string and return as integer."""
    if len(data_string) == 0:
        return 0
    elif "(" in data_string:
        n, *_ = data_string.split("(")
        number = re.sub(r"\D", "", n)
        return int(number)
    else:
        number = re.sub(r"\D", "", data_string)
        return int(number)


def modify_date(original_date):
    """Modify date string from ending with year to beginning with year."""
    d, m, y = original_date.split("-")
    return "-".join([y, m, d])


def clean_data(row):
    """Clean list of table-row strings and return as tuple."""
    date, _, c, d = row
    if date.endswith("2020"):
        date = modify_date(date)
    cases = get_number(c.strip())
    deaths = get_number(d.strip())
    return date, cases, deaths


def get_table_rows(wiki_table_html):
    """Extract table data from html and return as list of tuples."""
    # extract table from html
    table = wiki_table_html.find(
        "table", {"style": "text-align:left; border-collapse:collapse; width:100%;"}
    )
    rows = table.find_all("tr")
    table_data = []
    for row in rows[2:-1]:
        row_strings = [col.text for col in row.find_all("td")]
        if row_strings[0] != "⋮":
            table_data.append(clean_data(row_strings))
    return table_data


def create_df(wiki_table_html, country_code):
    """create dataframe from country data."""
    data = get_table_rows(wiki_table_html)
    cols = ["date", "cases_" + country_code, "deaths_" + country_code]
    df = pd.DataFrame(data, columns=cols)
    df.date = pd.to_datetime(df.date)
    last_date, _ = str(df.iloc[-1, 0]).split(" ")
    print(f"Data upto {last_date} collected for {country_names[country_code]}.")
    return df


def fill_missing_data(df):
    """Return dataframe with missing values filled in and numeric columns as integers."""
    # fix top row
    df.iloc[0, :] = df.iloc[0, :].fillna(0)
    # fill missing values
    df = df.fillna(method="ffill")
    df.iloc[:, 1:] = df.iloc[:, 1:].astype(int)
    return df


def download_data(countries):
    """Download corona data from wikipedia and return as pandas dataframe."""
    today = pd.to_datetime("today")
    yesterday = today - pd.DateOffset(days=1)
    # start date is when first case was reported in United States
    dates = pd.date_range(start="01-21-2020", end=yesterday)
    df = pd.DataFrame(dates, columns=["date"])
    print("Base dataframe created")
    soup_objects = get_wiki_pages(countries)
    country_codes = [wiki_shortcodes[c] for c in countries]
    for soup, country_code in zip(soup_objects, country_codes):
        country_data = create_df(soup, country_code)
        df = df.merge(country_data, how="left", on="date")
    print("Fill missing data.")
    df = fill_missing_data(df)
    print("Dataframe ready.")
    return df


worldometers_mapper = {'at': 'austria', 'be': 'belgium', 'ca': 'canada',
                       'dk': 'denmark', 'fr': 'france', 'de': 'germany', 'it': 'italy',
                       'jp': 'japan', 'nl': 'netherlands', 'kr': 'south korea',
                       'es': 'spain', 'se': 'sweden', 'uk': 'united kingdom',
                       'us': 'united states'}


def download_country_data():
    """Download worldometer country population and return as BeautifulSoup object"""
    url = 'https://www.worldometers.info/world-population/population-by-country/'
    populations = requests.get(url)
    populations.raise_for_status()
    return BeautifulSoup(populations.text, 'html.parser')


def get_country_table(bs4_object):
    """Extract population table from html and return as bs4 element tag."""
    table = bs4_object.find("table", {"id": "example2"})
    return table


def get_country_header_row(html):
    """Extract header row from html and return as list of strings."""
    headers = html.select('thead > tr > th')
    return [td_tag.text for td_tag in headers]


def get_country_table_rows(html):
    cols = []
    for row in html.select('tbody tr'):
        row_text = [x.text for x in row.find_all('td')]
        cols.append(row_text)
    return cols


def create_country_dataframe(rows, headers):
    """Return dataframe with only required columns, from list of lists of strings."""
    df = pd.DataFrame(rows, columns=headers)
    return df.iloc[:, [1, 2, 5, 6, 9, 10]]


def clean_country_dataframe(df):
    new_column_names = {'Country (or dependency)': 'country',
                        'Population (2020)': 'population',
                        'Density (P/Km²)': 'pop_density',
                        'Land Area (Km²)': 'land_area',
                        'Med. Age': 'median_age',
                        'Urban Pop %': 'urb_pop_pct'}
    df = df.rename(new_column_names, axis=1)
    df.country = df.country.str.lower()
    df.iloc[:, 1:] = df.iloc[:, 1:].replace(r'\D', '', regex=True)
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df.iloc[:, 1:] = df.iloc[:, 1:].astype('float64')
    return df


def get_country_data():
    page_html = download_country_data()
    table_html = get_country_table(page_html)
    headers = get_country_header_row(table_html)
    data_rows = get_country_table_rows(table_html)
    df = create_country_dataframe(data_rows, headers)
    df = clean_country_dataframe(df)
    return df


def population_table():
    country_data = get_country_data()
    pop_table = {}
    for code in worldometers_mapper.keys():
        pop = int(country_data[country_data.country == worldometers_mapper[code]].population)
        pop_table[code] = pop
    pop_table['ny'] = 19453561
    return pop_table
