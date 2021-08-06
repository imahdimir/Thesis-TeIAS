##
import warnings

# from multiprocess import Pool
from multiprocessing import freeze_support, cpu_count
from multiprocessing import Pool

from py import z_cf
from py import RSS
from py import z_ns

warnings.filterwarnings("ignore")

apw = z_ns.AdjPricesWithDates()
rss = z_ns.RSSParams()

evalSkipMonthSkipDay_0 = [(0, 0, 0), (0, 0, 7)]

fromMonthToMonth = [(138001, 139909), (138001, 139712), (138001, 139012),
                    (139301, 139712)]
evalMonths = [1, 2, 3, 6, 9, 12]
holdingMonths = [1, 2, 3, 6, 9, 12]
qCut = [5, 10]

all_configs_0 = z_cf.build_all_possible_configs(eval_day_skip_m_skip_d = evalSkipMonthSkipDay_0,
        from_month_to_month = fromMonthToMonth,
        eval_months = evalMonths,
        holding_months = holdingMonths,
        qcuts = qCut)

evalSkipMonthSkipDay_1 = [(0, 1, 0)]

fromMonthToMonth = [(138001, 139909), (138001, 139712), (138001, 139012),
                    (139301, 139712)]
evalMonths = [11]
holdingMonths = [1, 2, 3, 6, 9, 12]
qCut = [5, 10]

all_configs_1 = z_cf.build_all_possible_configs(eval_day_skip_m_skip_d = evalSkipMonthSkipDay_1,
        from_month_to_month = fromMonthToMonth,
        eval_months = evalMonths,
        holding_months = holdingMonths,
        qcuts = qCut)

all_configs = all_configs_0 + all_configs_1

def target1(kwargs):
    lts = RSS.MomentumStrategy(**kwargs)
    lts.run()

def main():
    pass
    ##
    adp = z_cf.get_adjprices()
    for el in evalSkipMonthSkipDay_0 + evalSkipMonthSkipDay_1:
        if not z_cf.make_relavant_data_feather_pathname(el[0],
                                                        el[1],
                                                        el[2]).is_file():
            z_cf.find_and_save_nth_day_and_skip_dates(adp, el[0], el[1], el[2])
    ##
    cores_n = 7
    # cores_n = 6
    print(f"Num of cores : {cores_n}")
    clusters = z_cf.return_clusters_indices(iterable_obj = all_configs,
                                            clustersize = cores_n)
    pool = Pool(cores_n)

    ##
    for i in range(0, len(clusters) - 1):
        start_i = clusters[i]
        end_i = clusters[i + 1]
        print(f"{start_i} to {end_i}")

        some_inp = all_configs[start_i:end_i]

        pool.map(target1, some_inp)

    ##

    ##

##


if __name__ == '__main__':
    freeze_support()
    main()
    print('Sc done!')
