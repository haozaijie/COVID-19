import pandas as pd
import os
import xlrd
import logging

logging.basicConfig(
    level='INFO', format="%(asctime)s:%(funcName)s:%(message)s")

columns = ['Province/State', 'Country/Region', 'Last Update', 'Confirmed']
output_columns = ['date', 'name', 'category', 'value']
path = os.path.dirname(os.getcwd(
)) + "/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"
US_MAPPING = pd.read_excel(
    os.getcwd() + "/US_MAPPING.xlsx", index_col='Postal')


def map_us_state(state, country):
    if pd.isna(state):
        pass
    elif "Diamond Princess" in state:
        state = "Diamond Princess"
    elif (len(state.split(", ")) == 2) & (country == 'US'):
        state = US_MAPPING.loc[state.split(", ")[1].strip()]['State']
    return state


def clean_raw_data(df):
    # standardize US state name
    df['Province/State'] = df.apply(lambda row: map_us_state(
        row['Province/State'], row['Country/Region']), axis=1)

    del df['Lat']
    del df['Long']

    return df


def stack_data(df):
    stacked = df.set_index(
        ['Province/State', 'Country/Region']).stack().reset_index()
    stacked.columns = columns

    stacked['Last Update'] = pd.to_datetime(stacked['Last Update']).apply(
        lambda row: row.strftime('%Y-%m-%d'))

    stacked.sort_values(by='Last Update', inplace=True)

    stacked.rename(columns={'Province/State': 'name',
                            'Country/Region': 'category',
                            'Last Update': 'date',
                            'Confirmed': 'value'},
                   inplace=True)
    stacked['name'].fillna('Not_specified', inplace=True)
    stacked = stacked.groupby(['category', 'name', 'date'])[
        'value'].sum().reset_index()

    return stacked


def filter_and_save(df, country, filename):
    filt = (df['category'].isin(country))
    output = df.loc[filt, output_columns]
    output['category'] = output['name']
    filenames = [filename, "csv"]
    output.to_csv(".".join(filenames), index=False)


def group_data(df):
    # stacked['name'].fillna('Not_specified', inplace=True)
    grouped = df.groupby(['category', 'date'])['value'].sum().reset_index()
    grouped['name'] = grouped['category']
    return grouped

    # "covid_19_ex_mainland_china_1.csv"


def main():
    logging.info('read raw data')

    # read data
    confirmed = pd.read_csv(path).drop_duplicates()

    logging.info('clean raw data')

    # clean data
    confirmed = clean_raw_data(confirmed)

    logging.info('stack cleaned data')

    # stack data by country/region and province/state
    stacked_state = stack_data(confirmed)

    logging.info('filter on data and save to file')

    # generate file for a particular country
    filter_and_save(stacked_state, ['US'], 'covid_19_daily_us_1')
    logging.info('US data saved')

    filter_and_save(
        stacked_state, ['Mainland China', 'China'], 'covid_19_daily_china_1')
    logging.info('China data saved')

    logging.info('group data on country level')

    # generate file on country level excluding mainland china
    grouped = group_data(stacked_state)
    filt = grouped['category'].isin(['Mainland China', 'China'])
    grouped.loc[-filt,
                output_columns].to_csv("covid_19_ex_mainland_china_1.csv", index=False)
    logging.info('country level data excluding mainladn china saved')


if __name__ == '__main__':
    main()
