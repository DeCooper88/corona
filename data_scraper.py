from bs4 import BeautifulSoup
import pandas as pd
import re
import requests as rq
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
        wiki_page = rq.get(url)
        wiki_page.raise_for_status()
        yield BeautifulSoup(wiki_page.text, "html.parser")
        pause_count += 1


def get_number(data_string):
    """Extract number from data_string and return as integer."""
    if len(data_string) == 0:
        return 0
    elif "(" in data_string:
        n, *_ = data_string.split("(")
        # number = n.replace(",", "")
        number = re.sub(r"\D", "", n)
        return int(number)
    else:
        # number = data_string.replace(",", "")
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
