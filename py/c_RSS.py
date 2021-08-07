##
import warnings

import pandas as pd

from py import z_classesFunctions as cf
from py import z_namespaces as ns


warnings.filterwarnings("ignore")

# namespaces
rs = ns.RSSParams()
ar = ns.AllResults()
ap = ns.AdjPricesWithDates()
po = ns.Portfolio()
dirs = ns.ProjectDirectories()

class RelativeStrengthStrategy:
    def __init__(self, **kwargs):
        self.params = kwargs

        self.res_dir_n = None
        self.res_dir = None
        self.res_buyhold = None
        self.res_rebalanced = None

        self.data = None
        self.formation_df = None
        self.trade_df = None

        self.cur_afs = None

        self.qBHPrts = None
        self.qRbPrts = None

        self.returnsBH = None
        self.returnsRb = None

    def set_paths(self):
        par = self.params

        self.res_dir_n = '-'.join([str(x) for x in list(self.params.values())])
        self.res_dir = dirs.RSSResults / self.res_dir_n
        self.res_buyhold = self.res_dir / ar.buy_and_hold
        self.res_rebalanced = self.res_dir / ar.rebalanced

        if not self.res_dir.is_dir():
            self.res_dir.mkdir()

            self.res_buyhold.mkdir()
            self.res_rebalanced.mkdir()

            for pnum in range(1, par[rs.quantiles] + 1):
                (self.res_buyhold / str(pnum)).mkdir()
                (self.res_rebalanced / str(pnum)).mkdir()

    def form_portfos(self):
        par = self.params
        self.qBHPrts = {x: cf.Portfolio() for x in
                        range(1, par[rs.quantiles] + 1)}
        self.qRbPrts = {x: cf.Portfolio() for x in
                        range(1, par[rs.quantiles] + 1)}

    def form_returns_dfs(self):
        par = self.params
        cols = [ap.afterSkipWDDate] + [x for x in
                                       range(1, par[rs.quantiles] + 1)]
        self.returnsBH = pd.DataFrame(columns = cols)
        self.returnsRb = pd.DataFrame(columns = cols)

    def load_data_from_feather(self):
        par = self.params
        fpn = cf.make_relavant_data_feather_pathname(
                formation_workday_each_month = par[rs.nthWDayEachMonthForEval],
                months_to_skip = par[rs.mMonthSkip],
                days_to_skip = par[rs.pDaysSkip])
        df = pd.read_feather(fpn)
        df = df[df[ap.jMonth].astype(int) >= par[rs.fromJMonth]]
        df = df[df[ap.jMonth].astype(int) <= par[rs.toJMonth]]
        df = df.sort_values(by = ap.date)
        self.data = df

    def find_formation_day_returns_and_bins(self):
        par = self.params
        df = self.data[self.data[ap.date].isin(self.data[ap.nthWDDate])]
        df = cf.compute_returns(df,
                                new_column_name = ap.jMonthsReturns,
                                groupby_list = [ap.id],
                                which_col_returns = ap.close,
                                how_many_periods = par[rs.evalMonths])
        df = cf.compute_bins_numbers(df,
                                     groupby_list = [ap.nthWDDate],
                                     which_col = ap.jMonthsReturns,
                                     quantiles = par[rs.quantiles])
        self.formation_df = df

    def add_bins_to_trade_df(self):
        df = self.data[self.data[ap.date].isin(self.data[ap.afterSkipWDDate])]
        df_1 = self.formation_df[
            [ap.id, ap.afterSkipWDDate, ap.binNo, ap.jMonthsReturns]]
        df = df.drop(columns = [ap.afterSkipWDDate])
        df = df.merge(df_1,
                      left_on = [ap.id, ap.date],
                      right_on = [ap.id, ap.afterSkipWDDate])
        self.trade_df = df

    def compute_monthly_returns(self):
        self.trade_df = cf.compute_returns(self.trade_df,
                                           new_column_name = ap.lastMonthReturns,
                                           groupby_list = [ap.id],
                                           which_col_returns = ap.close,
                                           how_many_periods = 1)

    def initialize(self):
        self.set_paths()
        self.form_portfos()
        self.form_returns_dfs()
        self.load_data_from_feather()
        self.find_formation_day_returns_and_bins()
        self.add_bins_to_trade_df()
        self.compute_monthly_returns()

    def remove_mature_postitions(self, k_before_afsk_date):
        for key in self.qBHPrts.keys():
            self.qBHPrts[key].remove_by_column_value(column_name = po.tradeDate,
                                                     value = k_before_afsk_date)
            self.qRbPrts[key].remove_by_column_value(column_name = po.tradeDate,
                                                     value = k_before_afsk_date)

    def trade(self, k_before_afsk_date):
        if k_before_afsk_date:
            self.remove_mature_postitions(k_before_afsk_date)

        afs = self.cur_afs
        for pno in self.qBHPrts.keys():
            port = self.trade_df[(self.trade_df[ap.date].eq(afs)) & (
                    self.trade_df[ap.binNo].eq(pno)) & (
                                         self.trade_df[ap.tradeHalt].eq(False))]
            # print(port)

            self.qBHPrts[pno].add_to_portfo(ids_list = port[ap.id],
                                            tikers_list = port[ap.ticker],
                                            trade_date = afs,
                                            j_month_returns_list = port[
                                                ap.jMonthsReturns])

            ids_to_remove = list(set(self.qRbPrts[pno].portfo[ap.id]) & set(
                    port[ap.id]))
            print(pno)
            print(ids_to_remove)
            not_emp = False
            for el_id in ids_to_remove:
                not_emp = True
                self.qRbPrts[pno].remove_by_column_value(column_name = ap.id,
                                                         value = el_id)

            self.qRbPrts[pno].add_to_portfo(ids_list = port[ap.id],
                                            tikers_list = port[ap.ticker],
                                            trade_date = afs,
                                            j_month_returns_list = port[
                                                ap.jMonthsReturns])
            if not_emp:
                assert not self.qBHPrts[pno].portfo.equals(
                        self.qRbPrts[pno].portfo)

    def record_portfos(self):
        afs = self.cur_afs
        for key in self.qBHPrts.keys():
            eqw_portfo_obj = self.qBHPrts[key]
            df = eqw_portfo_obj.portfo
            pn = self.res_buyhold / f"{key}" / f'{afs}{ns.xl_suf}'
            # print(pn)
            # print(df)
            # df = df.reset_index(drop=True)
            # df.to_feather(pn)
            df.to_excel(pn, index = False)

            eqw_portfo_obj = self.qRbPrts[key]
            df = eqw_portfo_obj.portfo
            pn = self.res_rebalanced / f'{key}' / f'{afs}{ns.xl_suf}'
            # df = df.reset_index(drop=True)
            # df.to_feather(pn)
            df.to_excel(pn, index = False)

    def record_last_month_eqw_returns(self):
        afs = self.cur_afs

        col0bh = {ap.afterSkipWDDate: afs}
        col0r = col0bh.copy()

        for prtn in self.qRbPrts.keys():
            col0bh[prtn] = self.qBHPrts[prtn].get_last_month_eq_weight_returns(
                    trade_df = self.trade_df,
                    end_of_month_date = afs)
            col0r[prtn] = self.qRbPrts[prtn].get_last_month_eq_weight_returns(
                    trade_df = self.trade_df,
                    end_of_month_date = afs)

        self.returnsBH = self.returnsBH.append(col0bh, ignore_index = True)
        self.returnsRb = self.returnsRb.append(col0r, ignore_index = True)

    def record(self, record_portfos: bool):
        self.record_last_month_eqw_returns()
        if record_portfos: self.record_portfos()

    def run(self, record_portfos: bool = False):
        par = self.params

        self.initialize()

        af_skp_dates = list(sorted(self.trade_df[ap.date].unique()))
        print(af_skp_dates)
        for ind, afs in enumerate(af_skp_dates):
            print(afs)
            self.cur_afs = afs

            self.record(record_portfos = record_portfos)

            if ind - par[rs.holdingMonths] >= 0:
                self.trade(af_skp_dates[ind - par[rs.holdingMonths]])
            else:
                self.trade(None)

        cf.save_df_to_xl(self.returnsBH, self.res_dir / ar.buy_and_hold)
        cf.save_df_to_xl(self.returnsRb, self.res_dir / ar.rebalanced)
        print('St done')

def main():
    pass
    ##
    adp = cf.get_adjprices()

    config = {
            rs.nthWDayEachMonthForEval: 0,
            rs.mMonthSkip             : 0,
            rs.pDaysSkip              : 7,
            rs.fromJMonth             : adp[ap.jMonth].min(),
            rs.toJMonth               : adp[ap.jMonth].max(),
            rs.evalMonths             : 3,
            rs.holdingMonths          : 3,
            rs.quantiles              : 5, }

    st1 = RelativeStrengthStrategy(**config)
    st1.initialize()
    ##
    st1.run()
    ##
    # % tests
    td1 = st1.data
    td2 = st1.formation_df
    td3 = st1.trade_df
    td4 = st1.qBHPrts
    ##
    td4 = cf.get_adjprices()
    td5 = td4[td4[ap.ticker].eq('وتوس')]
    td6 = td1[td1[ap.ticker].eq('وتوس')]
    ##
    df1 = pd.read_excel(
            '/Users/mahdimir/Documents/Thesis-TeIAS/RSSResults/0-0-0-138001'
            '-139909-6-3-10/buy_and_hold.xlsx')
    df2 = pd.read_excel(
            '/Users/mahdimir/Documents/Thesis-TeIAS/RSSResults/0-0-0-138001'
            '-139909-6-3-10/rebalanced.xlsx')
    print(df1.equals(df2))
    ##
    if not None:
        print('y')

##
if __name__ == "__main__":
    main()
else:
    pass
    ##
