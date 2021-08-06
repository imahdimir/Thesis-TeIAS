##

import pandas as pd
from persiantools.jdatetime import JalaliDate

from py import z_cf as fu
from py import z_ns as pa

def main():
    pass

    ##
    adp = fu.get_adjprices()
    apns = pa.AdjPricesWithDates()
    date_range = pd.date_range(start = adp[apns.date].min(),
                               end = adp[apns.date].max()).tolist()
    date_range = [x.date() for x in date_range]
    print(date_range)

    ##
    dates = pd.DataFrame(date_range, columns = ['allDays'])
    dates['jDate'] = dates[
        'allDays'].apply(lambda x: str(JalaliDate.to_jalali(x)))
    dates['WD'] = dates['allDays'].isin(adp[apns.date])

    fu.save_df_to_xl(dates, pa.clnData_dir / 'allDays')

    ##
