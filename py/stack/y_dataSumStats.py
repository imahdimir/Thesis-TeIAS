##
from datetime import datetime as dt

import pandas as pd
from dateutil.relativedelta import relativedelta

from py import z_cf as fu
from py import a_main as m
from py import b_makeFilledAdjPricesData as LastSc  # last script


ps = LastSc
script_name = fu.next_script_name(ps.ScN)

##
data = pd.read_parquet(m.CWD / "py" / "CleanData" / "{Sca.oupr}adjprices.parquet")

reldelta = relativedelta(dt.strptime(data['Date'].max(), "%Y-%m-%d"),
                         dt.strptime(data['Date'].min(), "%Y-%m-%d"))

gen_stats_dict = {
        'Dataset Columns'                               : str(list(data.columns)),
        'Data Frequency'                                : "Daily, Firm-Day",
        'Firm-Days (Number of All Data Rows)'           : len(data),
        'Number of Unique Firms in Whole Dataset'       : len(
                data['Ticker'].unique()),
        'From Date'                                     : data['Date'].min(),
        'From JDate'                                    : data['JDate'].min(),
        'To Date'                                       : data['Date'].max(),
        'To JDate'                                      : data['JDate'].max(),
        'Relative Delta-All Days(Not Just Working Days)': str(reldelta),
        'Unique Years'                                  : len(
                data['JYear'].unique()),
        'Unique Months'                                 : len(
                data['JYearMonth'].unique()),
        'Number of Working Days'                        : len(
                data['Date'].unique()),
        'Not Halted Firm-Days'                          : len(
                data[~data['TradeHalt']]),
        'Halted Firm Days (Filled Adj Price)'           : len(
                data[data['TradeHalt']]),
        '% Not Halted (Not Filled)'                     : len(
                data[~data['TradeHalt']]) / len(data),
        "% Halted"                                      : len(
                data[data['TradeHalt']]) / len(data),
        'Mean all Observation per Firm'                 : len(data) / len(
                data['Ticker'].unique()),
        'Mean Not Halted per Firm'                      : len(
                data[~data['TradeHalt']]) / len(data['Ticker'].unique()),
        'Mean Halted per Firm'                          : len(
                data[data['TradeHalt']]) / len(data['Ticker'].unique()), }

gen_stats = pd.DataFrame()
gen_stats[0] = gen_stats_dict.keys()
gen_stats[1] = gen_stats_dict.values()

gen_stats_pn_wo_suf = str(cwd / f'py/tables/{oupr}adjprices_metadata')
m.save_df_to_xl(gen_stats, gen_stats_pn_wo_suf, header = False)

##
# yearly stats
yrly = data.groupby('JYear')['Date'].nunique().to_frame().reset_index()
yrly = yrly.rename(columns = {"Date": "Working Days No."})

df = data.groupby('JYear')['ID'].nunique().to_frame().reset_index()
df = df.rename(columns = {"ID": "Unique Firms"})
yrly = yrly.merge(df)

df = data.groupby('JYear').size().reset_index()
df = df.rename(columns = {0: "Firm-Days"})
yrly = yrly.merge(df)

df = data[~data['TradeHalt']].groupby('JYear').size().reset_index()
df = df.rename(columns = {0: "Not Halted Firm-Days"})
yrly = yrly.merge(df)

df = data[data['TradeHalt']].groupby('JYear').size().reset_index()
df = df.rename(columns = {0: "Halted Firm-Days"})
yrly = yrly.merge(df)

yrly['% Not Halted'] = yrly['Not Halted Firm-Days'] / yrly['Firm-Days']
yrly['% Halted'] = yrly['Halted Firm-Days'] / yrly['Firm-Days']
yrly['Mean Firm-Days Per Firm'] = yrly['Firm-Days'] / yrly['Unique Firms']
yrly['Mean Not Halted Per Firm'] = yrly['Not Halted Firm-Days'] / yrly[
    'Unique Firms']
yrly['Mean Halted Per Firm'] = yrly['Halted Firm-Days'] / yrly['Unique Firms']

df = data.groupby('JYear')['Ticker'].unique().reset_index()
df['Ticker'] = df['Ticker'].apply(lambda x: str(x.tolist()))
yrly = yrly.merge(df)

yrly["IPO"] = None
for i in range(0, len(yrly) - 1):
    yrly.at[i + 1, 'IPO'] = str(list(set(eval(yrly.loc[i + 1]['Ticker'])) - set(
            eval(yrly.loc[i]['Ticker']))))

yrly['Removal'] = None
for i in range(0, len(yrly) - 1):
    yrly.at[i, 'Removal'] = str(list(set(eval(
            yrly.loc[i]['Ticker'])) - set(eval(yrly.loc[i + 1]['Ticker']))))

yrly['IPO No.'] = yrly[
    'IPO'].apply(lambda x: len(eval(x)) if x is not None else None)

yrl_stats = yrly.describe()
yrly1 = yrly.append(yrl_stats)

yrly1_pn = str(cwd / 'py/tables/adjprices_yearly_stats_table')
m.save_df_to_xl(yrly1, yrly1_pn, index = True)

##
# Monthly Stats
monthly = data.groupby(['JYearMonth']).size().reset_index()
monthly = monthly.rename(columns = {0: 'Firm-Days'})

df = data.groupby(['JYearMonth'])['Date'].nunique().reset_index()
df = df.rename(columns = {'Date': "Working Days No."})
monthly = monthly.merge(df)

df = data.groupby(['JYearMonth'])['ID'].nunique().reset_index()
df = df.rename(columns = {'ID': "Unique Firms"})
monthly = monthly.merge(df)

df = data[~data["TradeHalt"]].groupby(['JYearMonth']).size().reset_index()
df = df.rename(columns = {0: "Not Halted Firm-Days"})
monthly = monthly.merge(df)

df = data[data["TradeHalt"]].groupby(['JYearMonth']).size().reset_index()
df = df.rename(columns = {0: "Halted Firm-Days"})
monthly = monthly.merge(df)

monthly['% Not Halted'] = monthly['Not Halted Firm-Days'] / monthly['Firm-Days']
monthly['% Halted'] = monthly['Halted Firm-Days'] / monthly['Firm-Days']
monthly['Mean Firm-Days Per Firm'] = monthly['Firm-Days'] / monthly[
    'Unique Firms']
monthly['Mean Not Halted Per Firm'] = monthly['Not Halted Firm-Days'] / monthly[
    'Unique Firms']
monthly['Mean Halted Per Firm'] = monthly['Halted Firm-Days'] / monthly[
    'Unique Firms']

df = data.groupby('JYearMonth')['Ticker'].unique().reset_index()
df['Ticker'] = df['Ticker'].apply(lambda x: str(x.tolist()))

monthly = monthly.merge(df)
monthly['Ticker No.'] = monthly['Ticker'].apply(lambda x: len(eval(x)))

monthly['IPO'] = None
for i in range(0, len(monthly) - 1):
    monthly.at[i + 1, 'IPO'] = str(list(set(eval(
            monthly.loc[i + 1]['Ticker'])) - set(eval(
            monthly.loc[i]['Ticker']))))

monthly['Removal'] = None
for i in range(0, len(monthly) - 1):
    monthly.at[i, 'Removal'] = str(list(set(eval(
            monthly.loc[i]['Ticker'])) - set(eval(
            monthly.loc[i + 1]['Ticker']))))

monthly['IPO No.'] = monthly[
    'IPO'].apply(lambda x: len(eval(x)) if x is not None else None)

df = data.groupby('JYearMonth')['Date'].min().reset_index()
df = df.rename(columns = {'Date': '1st Day-Date'})
data = data.merge(df)

data_1st = data[data['Date'].eq(data['1st Day-Date'])]
data_1st = data_1st.sort_values('Date')
data_1st['Month Return'] = data_1st.groupby('ID')["Adj Close"].pct_change()
df = data_1st[data_1st['Month Return'].notna()].groupby('JYearMonth')[
    'Month Return'].mean().reset_index()
monthly = monthly.merge(df)
monthly = monthly.rename(columns = {
        'Month Return': 'Return on Equally Weighted Portfolio'})

monthly1 = monthly.append(monthly.describe())

monthly1_pn = str(cwd / 'py/tables/adjprices_monthly_stats_table')
m.save_df_to_xl(monthly1, monthly1_pn, index = True)

##
