##
from pathlib import Path
import pandas as pd
from tabulate import tabulate
from py import z_ns as ns


dirs = ns.ProjectDirectories()
gf = ns.GlobalFiles()
ar = ns.AllResults()
rs = ns.RSSParams()
jn = ns.JupyNBReports()

def main():
    ##
    allr = pd.read_excel(ns.CWD / f"{gf.allResults_Xl}{ns.xl_suf}")
    ##
    # %# JT93 - Table 1
    conf = {
            rs.nthWDayEachMonthForEval: [0],
            rs.mMonthSkip             : [0],
            rs.pDaysSkip              : [0, 7],
            rs.fromJMonth             : [138001],
            rs.toJMonth               : [139909],
            rs.evalMonths             : [3, 6, 9, 12],
            rs.holdingMonths          : [3, 6, 9, 12],
            rs.quantiles              : [10],
            ar.strategyType           : [ar.rebalanced],
            ar.binNo                  : [1, 10, 'w-l'], }

    def filter_results_by_config(config_dict, all_results=allr):
        filtered = all_results.copy()
        for k, v in config_dict.items():
            filtered = filtered[filtered[k].isin(v)]
        return filtered

    tbl1_0 = filter_results_by_config(conf)

    # tbl1_0
    ##
    def filter_table1_panel_a_or_b(tabl1, a_or_b):
        if a_or_b == 'a':
            skdays = 0
        elif a_or_b == 'b':
            skdays = 7
        else:
            raise 'a or b'
        pnl = tabl1[tabl1[ar.pDaysSkip].eq(skdays)]
        pnl = pnl[[ar.evalMonths, ar.holdingMonths, ar.binNo, ar.meanReturns,
                   ar.meanTStat, ar.meanPVal, ]]
        pnl.loc[pnl[ar.binNo].eq(1), jn.portfo_type] = jn.short
        pnl.loc[pnl[jn.portfo_type].eq(jn.short), 'l0'] = 0
        pnl.loc[pnl[ar.binNo].eq(10), jn.portfo_type] = jn.long
        pnl.loc[pnl[jn.portfo_type].eq(jn.long), 'l0'] = 1
        pnl.loc[pnl[ar.binNo].eq('w-l'), jn.portfo_type] = jn.longMinusShort
        pnl.loc[pnl[jn.portfo_type].eq(jn.longMinusShort), 'l0'] = 2
        pnl = pnl.drop(columns = ar.binNo)
        return pnl

    # Panel A
    tbl1_pnl_a = filter_table1_panel_a_or_b(tbl1_0, a_or_b = 'a')

    # tbl1_pnl_a
    ##
    def tabulate_panel(panel):
        pnl_r = pd.pivot_table(panel,
                               columns = ar.holdingMonths,
                               values = ar.meanReturns,
                               index = [ar.evalMonths, jn.portfo_type, 'l0'])
        pnl_t = pd.pivot_table(panel,
                               columns = ar.holdingMonths,
                               values = ar.meanTStat,
                               index = [ar.evalMonths, jn.portfo_type, 'l0'])
        pnl_p = pd.pivot_table(panel,
                               columns = ar.holdingMonths,
                               values = ar.meanPVal,
                               index = [ar.evalMonths, jn.portfo_type, 'l0'], )

        pnl_r["l"] = 0
        pnl_t["l"] = 1
        pnl_p["l"] = 2

        pnl_r = pnl_r.reset_index()
        pnl_t = pnl_t.reset_index()
        pnl_p = pnl_p.reset_index()

        pnl = pnl_r.append(pnl_t)
        pnl = pnl.append(pnl_p)

        pnl = pnl.sort_values(by = [ar.evalMonths, 'l0', "l"])

        pnl.loc[pnl['l'].isin([0, 2]), [3, 6, 9, 12]] = pnl.loc[
            pnl['l'].isin([0, 2]), [3, 6, 9,
                                    12]].applymap(lambda x: "{:.2f}%".format(100 * x))
        pnl.loc[pnl['l'].isin([1]), [3, 6, 9, 12]] = pnl.loc[
            pnl['l'].isin([1]), [3, 6, 9,
                                 12]].applymap(lambda x: "{:.4f}".format(x))

        pnl["r-t-p"] = pnl[
            "l"].apply(lambda x: "Returns" if x == 0 else "TStat" if x == 1 else "PVal")

        pnl = pnl[[ar.evalMonths, jn.portfo_type, "r-t-p", 3, 6, 9, 12]]

        print(tabulate(pnl,
                       showindex = False,
                       headers = ["J", "", "K", "3", "6", "9", "12"]))

    ##
    tabulate_panel(tbl1_pnl_a)
    # #%% md
    ### Insignificant Results at 5% in Panel A
    ##
    # tbl1_pnl_a_insig = tbl1_pnl_a[tbl1_pnl_a[ar.meanPVal] > 0.05]
    # tbl1_pnl_a_insig
    ##
    # print(tabulate(tbl1_pnl_a_insig, showindex = False, headers = 'keys'))
    ##
    # % Table 1-Panel B
    tbl1_pnl_b = filter_table1_panel_a_or_b(tbl1_0, a_or_b = 'b')
    # tbl1_pnl_b
    ## md
    ## Table 1 - Panel B
    ##
    tabulate_panel(tbl1_pnl_b)

    ##
    def are_all_zero_cost_portfolios_have_positive_return(table1=tbl1_0):
        all_p = table1.loc[
            table1[ar.binNo].eq('w-l'), ar.meanReturns].ge(0).all()
        return all_p

    all_zero_p = are_all_zero_cost_portfolios_have_positive_return()
    ## md
    ### Facts
    1.
    All
    Zero - cost
    portfolios(winners
    minus
    losers) have
    postive
    returns.

    Just
    Like
    JT(93).
    ##
    print(all_zero_p)

    ##
    def are_all_zero_cost_portfolios_have_sig_returns(table1=tbl1_0):
        all_s = table1.loc[
            table1[ar.binNo].eq('w-l'), ar.meanPVal].lt(0.05).all()
        # print(all_p)
        return all_s

    all_s_r = are_all_zero_cost_portfolios_have_sig_returns()
    ## md
    2.
    Are
    all
    zero - cost
    portfolios
    returns
    have
    significant
    returns
    at
    5 %?

    YES!
    ##
    print(all_s_r)
    ## md
    3.
    How
    t - stats
    are
    calculated?

    For
    each
    stratgey
    mean
    returns
    are
    calculated
    by
    taking
    average
    of
    all
    months
    returns, then
    its
    t - stat is calculated
    by
    its
    own
    sample
    of
    returns
    for each month assuming that returns are normal and the null hypothesis is that the mean return is zero.

    4.
    Which
    zero - cost
    strategy is the
    most
    successful?

    ##
    def find_most_succ_zero_cost_strat_in_pnl(table1):
        idxmax = table1[table1[jn.portfo_type].eq(jn.longMinusShort)][
            ar.meanReturns].idxmax()
        # print(idxmax)
        ose = table1.loc[idxmax].drop('l0')
        return ose.to_frame()

    ## md
    #### In Panel A:
    (9, 6)
    Strategy
    ##
    print(tabulate(find_most_succ_zero_cost_strat_in_pnl(tbl1_pnl_a)))
    ## md
    #### In Panel B:
    (9, 6)
    Strategy
    ##
    print(tabulate(find_most_succ_zero_cost_strat_in_pnl(tbl1_pnl_b)))
    ## md
    *Why
    all
    short
    strategies
    have
    negative
    returns? This is in the
    contrary
    with JT(93) examination of the US market.
    *Do
    JT(93)
    take
    short
    position
    on
    sell
    portfolio or long?
    ##
    if __name__ == '__main__':
        main()
        print('Sc done!')
    else:
        pass
        ##
