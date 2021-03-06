##
import pandas as pd
from py import z_cf as cf
from py import z_ns as ns
from py.c_RSS import RelativeStrengthStrategy
from scipy import stats
from multiprocess import Pool
from pathlib import Path


ap = ns.AdjPricesWithDates()
rs = ns.RSSParams()
ar = ns.AllResults()
gf = ns.GlobalFiles()

evalSkipMonthSkipDay = [(0, 0, 0), (0, 0, 7)]
fromMonthToMonth = [(138001, 139712), (138001, 139812), (138001, 138912)]
evalMonths = [3, 6, 9, 12]
holdingMonths = [3, 6, 9, 12]
quantiles = [5, 10]

conf_list_0 = cf.build_all_possible_configs(eval_day_skip_m_skip_d = evalSkipMonthSkipDay,
                                            from_month_to_month = fromMonthToMonth,
                                            eval_months = evalMonths,
                                            holding_months = holdingMonths,
                                            quantiles = quantiles)

all_configs = conf_list_0
print(all_configs)
print(len(all_configs))

class RelativeStrengthStrategyResults(RelativeStrengthStrategy):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        super().set_paths()

        self.bhxl = None
        self.rbxl = None

        self.bhxl_desc = None
        self.rbxl_desc = None

        self.not_cum_cols = None

    def read_monthly_returns_xl_files(self):
        self.bhxl = pd.read_excel(self.res_dir / f'{ar.buy_and_hold}.xlsx')
        self.rbxl = pd.read_excel(self.res_dir / f'{ar.rebalanced}.xlsx')

    def specify_not_cum_cols(self):
        par = self.params

        self.not_cum_cols = list(range(1, par[rs.quantiles] + 1)) + ['w-l']
        print(self.not_cum_cols)

    def remove_months_with_lower_than_k_portfolio(self):
        par = self.params

        required_lagged_returns = par[rs.evalMonths] + par[
            rs.holdingMonths] - 1

        self.bhxl = self.bhxl.iloc[required_lagged_returns:]
        self.rbxl = self.rbxl.iloc[required_lagged_returns:]

        self.bhxl = self.bhxl.reset_index(drop = True)
        self.rbxl = self.rbxl.reset_index(drop = True)

    def remove_until_first_full_cal_year(self):
        self.bhxl['month'] = self.bhxl[
            ap.afterSkipWDDate].apply(lambda x: x.month)

        self.bhxl['farvardin'] = self.bhxl['month'].eq(3)
        idx = self.bhxl['farvardin'].idxmax()
        print(idx)

        self.bhxl = self.bhxl.iloc[idx:]
        self.rbxl = self.rbxl.iloc[idx:]

    def set_date_as_index(self):
        self.bhxl = self.bhxl.set_index([ap.afterSkipWDDate])
        self.rbxl = self.rbxl.set_index([ap.afterSkipWDDate])

    def fillna_with_zero(self):
        self.bhxl = self.bhxl.fillna(0)
        self.rbxl = self.rbxl.fillna(0)

    def cal_winners_minus_losers_monthly_returns(self):
        par = self.params

        self.bhxl['w-l'] = self.bhxl[par[rs.quantiles]] - self.bhxl[1]
        self.rbxl['w-l'] = self.rbxl[par[rs.quantiles]] - self.rbxl[1]

    def cal_cum_returns(self):
        colorder = []
        for col in self.not_cum_cols:
            self.bhxl[f'{col}-cum'] = (self.bhxl[col] + 1).cumprod() - 1
            self.rbxl[f'{col}-cum'] = (self.rbxl[col] + 1).cumprod() - 1
            colorder += [col, f'{col}-cum']

        self.rbxl = self.rbxl[colorder]
        self.bhxl = self.bhxl[colorder]

    def save_cum_returns(self):
        cf.save_df_to_xl(self.bhxl, self.res_dir / ar.bh_cum, index = True)
        cf.save_df_to_xl(self.rbxl, self.res_dir / ar.rb_cum, index = True)

    def cal_tstat_and_describe_and_save(self):
        self.bhxl_desc = self.bhxl.describe()
        self.rbxl_desc = self.rbxl.describe()

        tstat_row_bh = []
        tstat_row_rb = tstat_row_bh.copy()
        pval_row_bh = tstat_row_bh.copy()
        pval_row_rb = tstat_row_bh.copy()

        for col in self.not_cum_cols:
            tst, pval = stats.ttest_1samp(list(self.bhxl[col]), 0)
            tstat_row_bh += [tst]
            pval_row_bh += [pval]

            tst, pval = stats.ttest_1samp(list(self.rbxl[col]), 0)
            tstat_row_rb += [tst]
            pval_row_rb += [pval]

        tsat_bh = pd.Series(data = tstat_row_bh,
                            index = self.not_cum_cols,
                            name = ar.meanTStat)
        pval_bh = pd.Series(data = pval_row_bh,
                            index = self.not_cum_cols,
                            name = ar.meanPVal)

        tsat_rb = pd.Series(data = tstat_row_rb,
                            index = self.not_cum_cols,
                            name = ar.meanTStat)
        pval_rb = pd.Series(data = pval_row_rb,
                            index = self.not_cum_cols,
                            name = ar.meanPVal)

        self.bhxl_desc = self.bhxl_desc.append([tsat_bh, pval_bh])
        self.rbxl_desc = self.rbxl_desc.append([tsat_rb, pval_rb])

        cf.save_df_to_xl(self.bhxl_desc,
                         self.res_dir / ar.bh_desc,
                         index = True)
        cf.save_df_to_xl(self.rbxl_desc,
                         self.res_dir / ar.rb_desc,
                         index = True)

    def run_1(self):
        self.read_monthly_returns_xl_files()
        self.specify_not_cum_cols()
        self.remove_months_with_lower_than_k_portfolio()
        self.remove_until_first_full_cal_year()
        self.set_date_as_index()
        self.fillna_with_zero()
        self.cal_winners_minus_losers_monthly_returns()
        self.cal_cum_returns()
        self.save_cum_returns()
        self.cal_tstat_and_describe_and_save()
        print(self.res_dir_n)

    def run_2(self):
        par = self.params

        bh_desc = pd.read_excel(self.res_dir / f'{ar.bh_desc}{ns.xl_suf}',
                                index_col = 0)
        rb_desc = pd.read_excel(self.res_dir / f'{ar.rb_desc}{ns.xl_suf}',
                                index_col = 0)

        res = []
        config = self.params.copy()
        for st_type, df in zip([ar.buy_and_hold, ar.rebalanced],
                               [bh_desc, rb_desc]):
            for bin_no in list(range(1, par[rs.quantiles] + 1)) + ['w-l']:
                config[ar.strategyType] = st_type
                config[ar.binNo] = bin_no
                config[ar.meanReturns] = df.loc['mean', bin_no]
                config[ar.meanTStat] = df.loc[ar.meanTStat, bin_no]
                config[ar.meanPVal] = df.loc[ar.meanPVal, bin_no]
                res.append(config.copy())
                print(res)
        return res

def target(config):
    mr = RelativeStrengthStrategyResults(**config)
    mr.run_1()

def target1(config):
    mr = RelativeStrengthStrategyResults(**config)
    return mr.run_2()

def main():
    pass
    ##
    aconf = all_configs[0]
    print(aconf)
    ams = RelativeStrengthStrategyResults(**aconf)
    ams.read_monthly_returns_xl_files()
    adf0 = ams.bhxl
    ams.specify_not_cum_cols()
    ams.remove_months_with_lower_than_k_portfolio()
    adf1 = ams.bhxl
    ams.remove_until_first_full_cal_year()
    adf = ams.bhxl
    ##
    cores_n = 6
    # cores_n = 6
    print(f"Num of cores : {cores_n}")
    clusters = cf.return_clusters_indices(iterable_obj = all_configs,
                                          clustersize = cores_n)
    pool = Pool(cores_n)
    ##
    for i in range(0, len(clusters) - 1):
        start_i = clusters[i]
        end_i = clusters[i + 1]
        print(f"{start_i} to {end_i}")

        some_inp = all_configs[start_i:end_i]

        pool.map(target, some_inp)
    ##
    all_results_df = pd.DataFrame()
    for inpu in all_configs:
        results = target1(inpu)
        all_results_df = all_results_df.append(results, ignore_index = True)

    cf.save_df_to_xl(all_results_df, Path(gf.allResults_Xl))

##
if __name__ == '__main__':
    main()
    print('Sc done!')
else:
    pass
    ##
