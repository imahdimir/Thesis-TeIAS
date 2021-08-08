##
import itertools

import pandas as pd

from py import relativeStrengthStrategies as jd93
from py import z_ns as pa
from py import z_cf as fu

from matplotlib import pyplot as plt
from matplotlib.ticker import MultipleLocator


apns = pa.AdjPricesWithDates()
spn = pa.RSSParams()

# basic strategy parameter values
base = pa.RSSParams()  # basic config values
# evalSkipMonthSkipDay = [(0, 0, 0), (0, 0, 7), (0, 1, 0)]
evalSkipMonthSkipDay = [(0, 0, 0), (0, 0, 7)]

# fromMonthToMonth = [(138001, 139909), (138001, 139201), (138001, 139801),
#                     (139301, 139801)]
fromMonthToMonth = [(138001, 139909)]
base.evalMonths = [3, 6, 9, 12]
base.holdingMonths = [3, 6, 9, 12]
base.quantiles = [10]
all_vals = [evalSkipMonthSkipDay, fromMonthToMonth, base.evalMonths,
            base.holdingMonths, base.quantiles]

all_sts = list(itertools.product(*all_vals))
print(all_sts)

all_configs = []
for el in all_sts:
    config = {
            spn.nthWDayEachMonthForEval: el[0][0],
            spn.mMonthSkip             : el[0][1],
            spn.pDaysSkip              : el[0][2],
            spn.fromJMonth             : el[1][0],
            spn.toJMonth               : el[1][1],
            spn.evalMonths             : el[2],
            spn.holdingMonths          : el[3],
            spn.quantiles              : el[4]}
    print(config)
    all_configs.append(config)

def plot_cum_returns(config):
    st1 = jd93.RelativeStrengthStrategy(**config)
    st1.initialize()
    bhpn = st1.res_dir / 'buy-and-hold.xlsx'
    bh = pd.read_excel(bhpn)
    bh = bh.fillna(0)
    bhcum = bh.copy()
    bhcum.iloc[:, 1:] = (1 + bhcum.iloc[:, 1:]).cumprod()
    bn = 'buy-and-hold-cumulative-returns'
    fu.save_df_to_xl(bhcum, st1.res_dir / f'{bn}.xlsx')

    fig, ax = plt.subplots()
    dates = bhcum.iloc[:, 0].to_frame()
    all_days = pd.read_excel(pa.clnData_dir / 'allDays.xlsx')
    all_days['allDays'] = all_days['allDays'].astype(str)
    dates = dates.merge(all_days,
                        left_on = dates.columns[0],
                        right_on = 'allDays')
    x1 = dates['jDate']
    ax.xaxis.set_major_locator(MultipleLocator(12))
    ax.set_xlabel('JDate')
    ax.set_ylabel('Fold')
    plt.xticks(rotation = 30)
    for col in bhcum.columns[1:]:
        p2 = ax.plot(x1, bhcum[col], label = str(col))

    ax.legend()
    fig.suptitle(f'long-{st1.res_dir_n}')
    plt.savefig(st1.res_dir / f'{bn}.png')
    plt.savefig(st1.res_dir / f'{bn}.svg')
    figpns = []
    figpns.append((st1.res_dir / f'{bn}.svg').resolve())

    bhcum = bh.copy()
    bhcum.iloc[:, 1:] = (1 - bhcum.iloc[:, 1:]).cumprod()
    bn = 'short-and-hold-cumulative-returns'
    fu.save_df_to_xl(bhcum, st1.res_dir / f'{bn}.xlsx')

    fig, ax = plt.subplots()
    dates = bhcum.iloc[:, 0].to_frame()
    all_days = pd.read_excel(pa.clnData_dir / 'allDays.xlsx')
    all_days['allDays'] = all_days['allDays'].astype(str)
    dates = dates.merge(all_days,
                        left_on = dates.columns[0],
                        right_on = 'allDays')
    x1 = dates['jDate']
    ax.xaxis.set_major_locator(MultipleLocator(12))
    ax.set_xlabel('JDate')
    ax.set_ylabel('Fold')
    plt.xticks(rotation = 30)
    for col in bhcum.columns[1:]:
        p2 = ax.plot(x1, bhcum[col], label = str(col))

    ax.legend()
    fig.suptitle(f'short-{st1.res_dir_n}')
    plt.savefig(st1.res_dir / f'{bn}.png')
    plt.savefig(st1.res_dir / f'{bn}.svg')
    figpns.append((st1.res_dir / f'{bn}.svg').resolve())

    bhcum = bh.copy()
    bhcum[f'{st1.quantiles} minus 1'] = bhcum[st1.quantiles] - bhcum[1]
    bn = f'{st1.quantiles} minus 1'
    fu.save_df_to_xl(bhcum, st1.res_dir / f'{bn}.xlsx')

    fig, ax = plt.subplots()
    dates = bhcum.iloc[:, 0].to_frame()
    all_days = pd.read_excel(pa.clnData_dir / 'allDays.xlsx')
    all_days['allDays'] = all_days['allDays'].astype(str)
    dates = dates.merge(all_days,
                        left_on = dates.columns[0],
                        right_on = 'allDays')
    x1 = dates['jDate']
    ax.xaxis.set_major_locator(MultipleLocator(12))
    ax.set_xlabel('JDate')
    ax.set_ylabel('Fold')
    plt.xticks(rotation = 30)
    ax.plot(x1, bhcum[f'{st1.quantiles} minus 1'])

    fig.suptitle(f'{st1.quantiles} minus 1-{st1.res_dir_n}')
    plt.savefig(st1.res_dir / f'{bn}.png')
    plt.savefig(st1.res_dir / f'{bn}.svg')
    figpns.append((st1.res_dir / f'{bn}.svg').resolve())

    return figpns

def main():
    pass

    ##
    all_figs = []
    for elc in all_configs:
        figpns = plot_cum_returns(elc)
        all_figs += figpns

    ##
    txt = '\n'.join([str(x.resolve()) for x in all_figs])
    with open('allpns.txt', 'w') as f:
        f.write(txt)

    ##
    st1 = jd93.RelativeStrengthStrategy(**all_configs[0])
    st1.initialize()
    bhpn = st1.res_dir / 'buy-and-hold.xlsx'
    bh = pd.read_excel(bhpn)
    bh = bh.fillna(0)
    bhcum = bh.copy()
    bhcum.iloc[:, 1:] = (1 + bhcum.iloc[:, 1:]).cumprod()
    bn = 'buy-and-hold-cumulative-returns'
    fu.save_df_to_xl(bhcum, st1.res_dir / f'{bn}.xlsx')

    fig, ax = plt.subplots()
    dates = bhcum.iloc[:, 0].to_frame()
    all_days = pd.read_excel(pa.clnData_dir / 'allDays.xlsx')
    all_days['allDays'] = all_days['allDays'].astype(str)
    dates = dates.merge(all_days,
                        left_on = dates.columns[0],
                        right_on = 'allDays')
    x1 = dates['jDate']
    ax.xaxis.set_major_locator(MultipleLocator(12))
    ax.set_xlabel('JDate')
    ax.set_ylabel('Fold')
    plt.xticks(rotation = 30)
    for col in bhcum.columns[1:]:
        p2 = ax.plot(x1, bhcum[col], label = str(col))

    ax.legend()
    fig.suptitle(f'long-{st1.res_dir_n}')
    plt.savefig(st1.res_dir / f'{bn}.png')
    plt.savefig(st1.res_dir / f'{bn}.svg')

    bhcum = bh.copy()
    bhcum.iloc[:, 1:] = (1 - bhcum.iloc[:, 1:]).cumprod()
    bn = 'short-and-hold-cumulative-returns'
    fu.save_df_to_xl(bhcum, st1.res_dir / f'{bn}.xlsx')

    fig, ax = plt.subplots()
    dates = bhcum.iloc[:, 0].to_frame()
    all_days = pd.read_excel(pa.clnData_dir / 'allDays.xlsx')
    all_days['allDays'] = all_days['allDays'].astype(str)
    dates = dates.merge(all_days,
                        left_on = dates.columns[0],
                        right_on = 'allDays')
    x1 = dates['jDate']
    ax.xaxis.set_major_locator(MultipleLocator(12))
    ax.set_xlabel('JDate')
    ax.set_ylabel('Fold')
    plt.xticks(rotation = 30)
    for col in bhcum.columns[1:]:
        p2 = ax.plot(x1, bhcum[col], label = str(col))

    ax.legend()
    fig.suptitle(f'short-{st1.res_dir_n}')
    plt.savefig(st1.res_dir / f'{bn}.png')
    plt.savefig(st1.res_dir / f'{bn}.svg')

    bhcum = bh.copy()
    bhcum[f'{st1.quantiles} minus 1'] = bhcum[st1.quantiles] - bhcum[1]
    bn = f'{st1.quantiles} minus 1'
    fu.save_df_to_xl(bhcum, st1.res_dir / f'{bn}.xlsx')

    fig, ax = plt.subplots()
    dates = bhcum.iloc[:, 0].to_frame()
    all_days = pd.read_excel(pa.clnData_dir / 'allDays.xlsx')
    all_days['allDays'] = all_days['allDays'].astype(str)
    dates = dates.merge(all_days,
                        left_on = dates.columns[0],
                        right_on = 'allDays')
    x1 = dates['jDate']
    ax.xaxis.set_major_locator(MultipleLocator(12))
    ax.set_xlabel('JDate')
    ax.set_ylabel('Fold')
    plt.xticks(rotation = 30)
    ax.plot(x1, bhcum[f'{st1.quantiles} minus 1'])

    fig.suptitle(f'{st1.quantiles} minus 1-{st1.res_dir_n}')
    plt.savefig(st1.res_dir / f'{bn}.png')
    plt.savefig(st1.res_dir / f'{bn}.svg')

    ##  ##
