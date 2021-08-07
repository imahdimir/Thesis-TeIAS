##
import warnings

# from multiprocess import Pool
from multiprocessing import freeze_support, cpu_count
from multiprocessing import Pool

from py import z_classesFunctions as cf
from py import c_RSS
from py import z_namespaces as ns


warnings.filterwarnings("ignore")

apw = ns.AdjPricesWithDates()
rss = ns.RSSParams()

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

# evalSkipMonthSkipDay_1 = [(0, 1, 0)]
# fromMonthToMonth = [(138001, 139909), (138001, 139712), (138001, 139012),
#                     (139301, 139712)]
# evalMonths = [11]
# holdingMonths = [1, 2, 3, 6, 9, 12]
# qCuts = [5, 10]
#
# all_configs_1 = cf.build_all_possible_configs(eval_day_skip_m_skip_d = evalSkipMonthSkipDay_1,
#                                               from_month_to_month = fromMonthToMonth,
#                                               eval_months = evalMonths,
#                                               holding_months = holdingMonths,
#                                               qcuts = qCuts)
#
# evalSkipMonthSkipDay = [(0, 0, 0)]
# fromMonthToMonth = [(138001, 138912)]
# evalMonths = [3]
# holdingMonths = [3]
# qCuts = [10]

# all_configs_2 = cf.build_all_possible_configs(eval_day_skip_m_skip_d = evalSkipMonthSkipDay,
#                                               from_month_to_month = fromMonthToMonth,
#                                               eval_months = evalMonths,
#                                               holding_months = holdingMonths,
#                                               quantiles = quantiles)

all_configs = conf_list_0  # + all_configs_1
print(all_configs)
print(len(all_configs))

def target1(kwargs):
    lts = c_RSS.RelativeStrengthStrategy(**kwargs)
    lts.run()

def main():
    pass
    ##
    adp = cf.get_adjprices()
    for el in evalSkipMonthSkipDay:
        if not cf.make_relavant_data_feather_pathname(el[0],
                                                      el[1],
                                                      el[2]).is_file():
            cf.find_and_save_nth_day_and_skip_dates(adp, el[0], el[1], el[2])
    ##
    cores_n = cpu_count()
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

        pool.map(target1, some_inp)
    ##

##
if __name__ == '__main__':
    freeze_support()
    main()
    print(f'{__file__} done!')
else:
    pass
    ##
