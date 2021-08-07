# %%
import z_main_a as a
import pandas as pd
import matplotlib.pyplot as plt


def main():
    pass

    # %%

    adjPrices = pd.read_parquet(a.filAdjPricesPn)
    df1 = adjPrices[~adjPrices[a.tradeHaltC]]
    df1

    # %%
    ids_month = df1.groupby(a.iJMC)[a.idC].unique()
    ids_month

    # %%
    ids_month_count = ids_month.apply(len)
    ids_month_count

    # %%
    x1 = ids_month_count.index.astype(str)
    y1 = ids_month_count

    fig, ax = plt.subplots()
    p2 = ax.bar(x1,
                y1)
    ax.xaxis.set_major_locator(MultipleLocator(12))
    fig.suptitle('Number of Unique Symbols with No Trading Halt at the Start '
                 'of Each Month',
                 fontsize = 14,
                 fontweight = 'bold')
    ax.set_xlabel('Jalali Month')
    ax.set_ylabel('Number')
    ax.tick_params(axis = 'x',
                   rotation = 70)

    rects = ax.patches
    labels = y1

    for i, rec_lbl in enumerate(zip(rects,
                                    labels)):
        if i % 12 == 0 or i == len(y1) - 1:
            height = rec_lbl[0].get_height()
            ax.text(rec_lbl[0].get_x() + rec_lbl[0].get_width() / 2,
                    height + 20,
                    rec_lbl[1],
                    ha = 'center',
                    va = 'bottom')

    # %%
    s1 = sa.RelativeStrengthStrategy(1,
                                     0,
                                     0,
                                     138001,
                                     139909,
                                     (1, 0),
                                     1,
                                     1,
                                     True,
                                     False)
    s1.proceed()
    s1logf = StLog(a.resBaktestD + '/1_0_0_138001_139909_(1, 0)_1_1_True_False')
    s1df1 = s1logf.res
    s1mr = s1logf.month_rets

    # %%
    x2 = s1mr[a.tradeMonEndJmonth]
    y2 = s1mr[a.retMean]
    y3 = 100 * y2

    # %%
    fig, ax = plt.subplots()
    p2 = ax.plot(x2,
                 y3)
    ax.xaxis.set_major_locator(MultipleLocator(12))
    fig.suptitle('Monthly Return of Equally Weighted Portfolio of All '
                 'Securities in Each Month',
                 fontsize = 14,
                 fontweight = 'bold')
    ax.set_xlabel('Jalali Month')
    ax.set_ylabel('Return %')
    ax.grid()

    # %%
    df2 = pd.DataFrame({
            'x': x2,
            'y': y2})
    df2['yr'] = df2['x'].apply(lambda x: x[:4])
    df2['1+r'] = 1 + df2['y']
    df3 = df2.groupby('yr')['1+r'].prod().to_frame().reset_index()
    df3['r'] = df3['1+r'] - 1

    # %%
    x3 = df3['yr'].astype(str)
    y4 = df3['r'] * 100

    fig, ax = plt.subplots()
    p3 = ax.plot(x3,
                 y4)
    fig.suptitle('Annual Returns of Equally Weighted Portfolio of All '
                 'Securities in Each Month',
                 fontsize = 14,
                 fontweight = 'bold')
    ax.set_xlabel('Jalali Month')
    ax.set_ylabel('Return %')
    ax.grid()

    # %%
    y5 = y2 + 1
    z1 = np.cumprod(y5)

    # %%
    fig, ax = plt.subplots()
    p4 = ax.plot(x2,
                 z1)
    fig.suptitle('Cumulative Returns of Equally Weighted Portfolio of All '
                 'Securities',
                 fontsize = 14,
                 fontweight = 'bold')
    ax.set_xlabel('Jalali Month')
    ax.set_ylabel('Fold')
    ax.grid()
    ax.xaxis.set_major_locator(MultipleLocator(12))
    ax.yaxis.set_ticks([1] + [100 * w for w in range(1,
                                                     21)])
    rects = ax.patches
    labels = z1

    for i, rec_lbl in enumerate(zip(rects,
                                    labels)):
        if i % 12 == 0 or i == len(y5) - 1:
            height = rec_lbl[0].get_y()
            ax.text(rec_lbl[0].get_x(),
                    height + 10,
                    rec_lbl[1],
                    ha = 'center',
                    va = 'bottom')

    # %%
    m_no = len(y2)
    m_no

    # %%
    yr_no = len(df3)
    yr_no

    # %%
    m_mean = np.mean(y3)
    m_mean

    # %%
    yr_mean = np.mean(y4)
    yr_mean

    # %%
    m_var = np.var(y2)
    m_var

    # %%
    yr_var = np.var(df3['r'])
    yr_var

    # %%
    m_std = np.std(y2)
    m_std

    # %%
    yr_std = np.std(df3['r'])
    yr_std

    # %%
    m_pr = len([w for w in y2 if w > 0])
    m_pr

    # %%
    yr_pr = df3[df3['r'].gt(0)].count()
    yr_pr

    # %%
    m_nr = len([w for w in y2 if w < 0])
    m_nr

    # %%
    yr_nr = df3[df3['r'].lt(0)].count()
    yr_nr

    # %%
    m_median = np.median(y2)
    m_median * 100

    # %%
    yr_median = np.median(df3['r'])
    yr_median * 100

    # %%
    m_max = np.max(y2)
    m_max

    # %%
    yr_max = np.max(df3['r'])
    yr_max

    # %%
    m_min = np.min(y2)
    m_min

    # %%
    yr_min = np.min(df3['r'])
    yr_min

    # %%
    m_cumr = z1.iloc[-1] - 1
    m_cumr * 100

    # %%
    df4 = df3.iloc[:-1]
    yr_cum = df4['1+r'].prod() - 1
    yr_cum * 100

    # %%
    fig, ax = plt.subplots()
    x4 = [1 * w for w in range(-4 * 5,
                               11 * 5)]
    p5 = ax.hist(y3,
                 bins = x4,
                 density = True)
    ax.set_xlabel('% Return')
    ax.set_ylabel('% of observations')
    # ax.set_yticklabels([w for w in range(0, 13)])
    ax.xaxis.set_major_locator(MultipleLocator(5))
    ax.xaxis.set_minor_locator(MultipleLocator(1))

    ax.yaxis.set_major_formatter(PercentFormatter(1))

    mu1, std1 = norm.fit(y3)
    p6 = norm.pdf(x4,
                  mu1,
                  std1)
    plt.plot(x4,
             p6,
             'k',
             linewidth = 2)

    ax.legend(['Fitted Normal Distribution', 'Monthly Returns Distribution'])
    ax.grid()

    # %%
    rf_daily = pd.read_excel(f'{a.indataD}/risk-free-rate-1399-05-06.xlsx')
    rf_daily

    # %%
    rf_daily1 = rf_daily.iloc[7:, 1:]
    rf_daily1

    # %%
    rf1cols = rf_daily1.columns
    rf_daily1 = rf_daily1.rename(columns = {
            rf1cols[0]: a.dateC,
            rf1cols[1]: 'JDate',
            rf1cols[2]: 'r_f'})
    rf_daily1

    # %%
    rf_daily1[a.iJDC] = rf_daily1[
        'JDate'].apply(lambda x: int(str(x).replace('/',
                                                    '')))
    rf_daily1

    # %%
    rf_daily2 = rf_daily1[[a.dateC, a.iJDC, 'r_f']]
    rf_daily2

    # %%
    rf_daily2[a.iJMC] = rf_daily2[a.iJDC] // 100

    # %%
    def to_float(x):
        try:
            return float(x)
        except ValueError:
            return np.nan

    rf_daily2['r_f'] = rf_daily2['r_f'].apply(lambda x: to_float(x))

    # %%
    rf_daily2['r_f'] = rf_daily2['r_f'].fillna(0)

    # %%
    rf_m1 = rf_daily2.groupby(a.iJMC)['r_f'].mean()
    rf_m1 = rf_m1

    # %%

    # %%

    # %%

    # %%
    one_percent_sig = results_df[results_df['r_1samp_pvlaue'] * 100 < 1]
    len(one_percent_sig)

    # %%
    most_sig_100 = results_df.sort_values('r_1samp_tstat',
                                          ascending = False).head(100)
    most_sig_100

    # %%
    df6 = most_sig_100.head(10)

    # %%
    df6['Returns Mean'] = df6['r_mean_mean'].apply(lambda x: str(x * 100)[
                                                             0:4] + '\%')
    df6['P-Value'] = df6['r_1samp_pvlaue'].apply(lambda x: str(format(x,
                                                                      '.5f')))
    df6['STD'] = df6['r_tstd'].apply(lambda x: str(x)[0:5])
    df6 = df6.rename(columns = {
            no(a.evalWdayEachMonth)     : 'Evaluation Day Each Month',
            no(a.monSkip)               : 'Months To Skip',
            no(a.daySkip)               : 'Days To Skip',
            no(a.evalMonthsPair0) + '_0': 'Evaluation Period Include',
            no(a.evalMonthsPair0) + '_1': 'Evaluation Period Exclude',
            no(a.holdPeriod)            : 'Holding Period Months',
            no(a.qcuts)                 : 'Qcut',
            no(a.doesBuy)               : 'Long',
            no(a.doesShort)             : 'Short',
            })

    # %%
    df7 = df6[
        ['Evaluation Day Each Month', 'Months To Skip', 'Days To Skip',
         'Evaluation Period Include',
         'Evaluation Period Exclude', 'Holding Period Months', 'Qcut', 'Long',
         'Short',
         'Returns Mean', 'P-Value',
         'STD']]
    df7

    # %%
    tex1 = df7.to_latex(index = False,
                        escape = False,
                        header = [
                                '\\rotatebox{90}{\\tiny{' + c + '}}'
                                for c in
                                df7.columns])
    tex1

    # %%
    configs1 = []
    for i, r in df7.iterrows():
        dct = r[:9].to_dict()
        configs1.append(dct)
    configs1

    # %%
    for i in range(0,
                   len(configs1)):
        configs1[i]['Start Month'] = 138001
        configs1[i]['End Month'] = 139909
        configs1[i][
            'Eval'] = f'({configs1[i]["Evaluation Period" \
                                      " Include"]}, {configs1[i]["Evaluation " \
            "Period Exclude"]})'
        if configs1[i]['Long']:
            configs1[i]['long'] = True
        else:
            configs1[i]['long'] = False
        if configs1[i]['Short']:
            configs1[i]['short'] = True
        else:
            configs1[i]['short'] = False

        # %%
        pns1 = []
        for e in configs1:
            pn = f'{e["Evaluation Day Each Month"]}_{e["Months To Skip"]}_{e[
        "Days To Skip"]}_
        {e["Start Month"]}
        _
        {e["End Month"]}
        _
        {e["Eval"]}
        _
        {e["Holding Period Months"]}
        _
        {e["Qcut"]}
        _
        {e["long"]}
        _
        {e["short"]}
        '
        pns1.append(pn)

        # %%
        fig, ax = plt.subplots()
        ax.xaxis.set_major_locator(MultipleLocator(12))

        for e in pns1:
            obj = StLog(a.resBaktestD + f'/{e}')
        mrets = obj.month_rets
        x5 = mrets[a.tradeMonEndJmonth]
        y5 = mrets[a.retMean] * 100
        ax.plot(x5,
                y5)

        fig.suptitle('Monthly Returns for 10 Most Significant Strategies',
                     fontsize = 14,
                     fontweight = 'bold')
        ax.set_ylabel('% Return')

        # %%
        fig, ax = plt.subplots()
        ax.xaxis.set_major_locator(MultipleLocator(12))

        for e in pns1:
            obj = StLog(a.resBaktestD + f'/{e}')
        mrets = obj.month_rets
        x5 = mrets[a.tradeMonEndJmonth]
        mrets.iloc[0] =
        y5 = mrets[a.retMean] * 100
        ax.plot(x5,
                y5)

        fig.suptitle('Monthly Returns for 10 Most Significant Strategies',
                     fontsize = 14,
                     fontweight = 'bold')
        ax.set_ylabel('% Return')

        # %%
        fig, ax = plt.subplots()
        x6 = [1 * w for w in range(-4 * 5,
                                   11 * 5)]
        y6 = one_percent_sig['r_mean_mean'] * 100

        p7 = ax.hist(y6,
                     bins = x6,
                     density = True)
        ax.set_xlabel('% Return')
        ax.set_ylabel('% of Strategies')
        ax.xaxis.set_major_locator(MultipleLocator(5))
        ax.xaxis.set_minor_locator(MultipleLocator(1))
        ax.yaxis.set_major_formatter(PercentFormatter(1))

        fig.suptitle('Distributions of Returns in All 1% Significant '
                     'Strategies',
                     fontsize = 14,
                     fontweight = 'bold')

        # %%
        ov_ind = pd.read_csv(a.indataD + f'/indexes_1399-09-28.csv')
        ov_ind = ov_ind[ov_ind['index_id'].eq('overall_index')]
        ov_ind

        # %%
        ov_ind_1 = ov_ind[['date', 'index']]

        # %%

    def convert_date(idt):
        lyr = idt.split('/')[0]
        lm0 = idt.split('/')[1]
        if int(lm0) < 10:
            lm = f'0{str(lm0)}'
        else:
            lm = lm0
        return lyr + lm

    # %%
    ov_ind_1['Month'] = ov_ind_1['date'].apply(convert_date)

    # %%
    ov_ind_2 = ov_ind_1.drop_duplicates(subset = 'Month')

    # %%
    for e in pns1:
        obj = StLog(a.resBaktestD + f'/{e}')
        mrets = obj.month_rets[[a.tradeMonEndJmonth, a.retMean]]
        mrets = mrets.rename(columns = {
                a.retMean          : str(list(obj.config.values())),
                a.tradeMonEndJmonth: 'Month'})
        ov_ind_2 = ov_ind_2.merge(mrets,
                                  how = 'left')

    # %%
    ov_ind_2['indexg'] = ov_ind_2['index'].pct_change()

    # %%
    ov_ind_2.iloc[:, 3:] += 1

    # %%
    ov_ind_2.iloc[0, 3:] = 1

    # %%
    ov_ind_3 = ov_ind_2.iloc[:, 2:]

    # %%
    ov_ind_3.iloc[:, 1:] = ov_ind_3.iloc[:, 1:].cumprod()

    # %%
    fig, ax = plt.subplots()
    x7 = ov_ind_3.iloc[:, 0].astype(str)
    for i in list(range(1,
                        6)) + [11]:
        ax.plot(x7,
                ov_ind_3.iloc[:, i])

    fig.suptitle('Relative Comparison Between Overall Index and Strategies',
                 fontsize = 14,
                 fontweight = 'bold')
    ax.set_ylabel('Fold')
    ax.grid()
    ax.xaxis.set_major_locator(MultipleLocator(12))
    ax.yaxis.set_ticks([1] + [100 * w for w in range(1,
                                                     21)])
    ax.legend(list(ov_ind_3.iloc[:, 1:6].columns) + ['Overall Index'])

    # %%
