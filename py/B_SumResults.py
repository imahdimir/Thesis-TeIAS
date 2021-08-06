##
import main as b
from glob import glob
import pandas as pd
from multiprocessing import cpu_count
# from multiprocessing import Pool
from multiprocess import Pool
from varname import nameof as n
import os
from scipy import stats
import pickle

with open('configItems.pickle',
          'rb') as f:
    configItems = pickle.load(f)


class StLog:

    def __init__(self,
                 pn):
        bn = os.path.basename(pn).split('.')[0]
        cp = bn.split('_')
        self.config = [eval(x) for x in cp]
        self.config_dict = {x: y for x, y in zip(configItems,
                                                 self.config)}
        print(self.config_dict)
        self.df = pd.read_excel(pn)

        self.df1 = self.df[
            (self.df["HoldingPeriod_Start"].ne(0)) & (
                    self.df["HoldingPeriod_End"].ne(0))]
        self.df1 = self.df1.iloc[self.config_dict["formationMonths"]:, :]
        self.df1 = self.df1.fillna(0)

        df1 = self.df1
        cd = self.config_dict
        df1["BuyMinusSellPosReturn"] = df1["LongPosReturn"] - df1[
            "ShortPosReturn"]

        for el in ["Long", "Short", "BuyMinusSell"]:
            cd[f"{el}PosReturn"] = df1[f"{el}PosReturn"].mean()
            cd[f"{el}PosTStat"], cd[f"{el}PosPVal"] = stats.ttest_1samp(
                    df1[f"{el}PosReturn"],
                    0)

        self.entry = cd
        self.df1 = df1


def target1(pn):
    st = StLog(pn)
    if st.config_dict['toYearMonth'] == 139712:
        return st.entry
    else:
        return None


##
logpns = glob('backtest_results/*.xlsx')
print(logpns)
print(len(logpns))

##
res = pd.DataFrame()
for xl in logpns:
    ent = target1(xl)
    res = res.append(ent,
                     ignore_index=True)

##
res1 = res.copy()

##
res1 = res1.convert_dtypes()
cols = []
for el1 in ["Long", "Short", "BuyMinusSell"]:
    cols.append(f"{el1}PosReturn")
    cols.append(f"{el1}PosTStat")
    cols.append(f"{el1}PosPVal")
cols
##
res1 = res1[configItems + cols]
res1
##
res1.to_excel('table1/results_sum_1.xlsx',
              index=False)

##
