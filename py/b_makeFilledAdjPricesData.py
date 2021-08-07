##
from datetime import datetime as dt

import pandas as pd
from persiantools.jdatetime import JalaliDate

from py import z_namespaces as ns


# Shortened Namespaces
dirs = ns.ProjectDirectories()
iap = ns.InputCsvAdjPrices()
ap = ns.AdjPrices()
ic = ns.InputCsvIdSym()
gf = ns.GlobalFiles()

def main():
    pass
    ##
    adjp_in_pn = list(dirs.adjPricesInputOneCsv.glob("*.csv"))[0]
    df = pd.read_csv(adjp_in_pn)
    print(df)

    ren_dict = {
            iap.ID    : ap.id,
            iap.Date  : ap.date,
            iap.High  : ap.high,
            iap.Low   : ap.low,
            iap.Open  : ap.open,
            iap.Last  : ap.last,
            iap.Volume: ap.volume,
            iap.close : ap.close, }

    df = df.rename(columns = ren_dict)
    df = df[list(ren_dict.values())]

    df = df.drop_duplicates(subset = [ap.id, ap.date])

    df[ap.id] = df[ap.id].astype(int)
    df[ap.id] = df[ap.id].astype(str)
    ##
    uniq_ids = df[ap.id].unique()
    uniq_dates = df[ap.date].unique()
    uniq_ids = pd.DataFrame(data = uniq_ids, columns = [ap.id])
    uniq_dates = pd.DataFrame(data = uniq_dates, columns = [ap.date])
    ##
    tickers = pd.read_csv(list(dirs.idSymInputOneCsv.glob('*.csv'))[0])
    tickers = tickers.rename(columns = {ic.Id_tse: ap.id, ic.name: ap.ticker, })
    tickers = tickers[[ap.id, ap.ticker]]
    tickers = tickers.dropna()
    tickers[ap.ticker] = tickers[ap.ticker].apply(lambda x: str(x).replace(
            "\u200c",
            ''))
    tickers[ap.ticker] = tickers[ap.ticker].str.strip()

    tickers[ap.id] = tickers[ap.id].astype(int)
    tickers[ap.id] = tickers[ap.id].astype(str)
    ##
    uniq_ids = uniq_ids.merge(tickers)

    ids_cross_dates = pd.merge(uniq_ids, uniq_dates, how = 'cross')
    ##
    df = df.merge(ids_cross_dates, how = 'outer')

    df = df.sort_values(ap.date)
    df[ap.close] = df.groupby(ap.id)[ap.close].fillna(method = 'ffill')
    df = df.dropna(subset = [ap.close])

    df[ap.tradeHalt] = df[ap.volume].isna()

    df[ap.date] = df[ap.date].apply(lambda x: dt.strptime(x, "%Y-%m-%d").date())
    df[ap.jDate] = df[ap.date].apply(lambda x: str(JalaliDate.to_jalali(x)))
    df[ap.jMonth] = df[ap.jDate].apply(lambda x: int(str(x)[:7].replace('-',
                                                                        '')))

    df = df.convert_dtypes()
    print(df)

    assert not df.duplicated(subset = [ap.id, ap.date]).all()
    assert not df[ap.ticker].isna().all()

    df = df[list(ap.__dict__.values())]

    pn = gf.adjPricesPrq
    print(df)
    df.to_parquet(pn, index = False)
    print(pn)

##
if __name__ == '__main__':
    main()
    print(f"""
    
        
          Finish.
          Sc. {__file__} done.""")
else:
    pass
    ##
