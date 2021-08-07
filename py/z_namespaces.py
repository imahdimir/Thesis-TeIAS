##
from pathlib import Path
from varname import nameof as no


# constants
xl_suf = '.xlsx'
parquet_suf = '.parquet'

CWD = Path('.')  # project's main dir

class ProjectDirectories:
    def __init__(self):
        self.py = None
        self.cache = None
        self.cleanData = None
        self.inputData = None
        self.RSSResults = None
        self.adjPricesInputOneCsv = None
        self.idSymInputOneCsv = None

        for attr_key in self.__dict__:
            self.__dict__[attr_key] = CWD / attr_key

        for attr_val in self.__dict__.values():
            if not attr_val.is_dir():
                attr_val.mkdir()

class InputCsvAdjPrices:
    def __init__(self):
        self.ID = None
        self.Date = None
        self.High = None
        self.Low = None
        self.Open = None
        self.Last = None
        self.Volume = None
        self.close = None

        for attr_key in self.__dict__:
            self.__dict__[attr_key] = attr_key

class InputCsvIdSym:
    def __init__(self):
        self.Id_tse = None
        self.name = None

        for attr_key in self.__dict__:
            self.__dict__[attr_key] = attr_key

class GlobalFiles(ProjectDirectories):
    def __init__(self):
        super().__init__()
        self.adjPricesPrq = None
        self.allResults_Xl = None

        for attr in self.__dict__:
            self.__dict__[attr] = attr

        self.adjPricesPrq = self.cleanData + f'/{no(self.adjPricesPrq)}{parquet_suf}'

class AdjPrices:
    def __init__(self):
        self.id = None
        self.ticker = None
        self.jMonth = None
        self.jDate = None
        self.date = None
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.last = None
        self.volume = None
        self.tradeHalt = None

        for attr in self.__dict__:
            self.__dict__[attr] = attr

class AdjPricesWithDates(AdjPrices):
    def __init__(self):
        super().__init__()
        self.nthWD = None
        self.nthWDJDate = None
        self.nthWDDate = None
        self.mMonthSkip = None
        self.pDaysSkip = None
        self.afterSkipWDDate = None
        self.afterSkipWDJDate = None
        self.jMonthsReturns = None
        self.binNo = None
        self.lastMonthReturns = None

        for attr in self.__dict__:
            self.__dict__[attr] = attr

class RSSParams:
    def __init__(self):
        self.nthWDayEachMonthForEval = None
        self.mMonthSkip = None
        self.pDaysSkip = None
        self.fromJMonth = None
        self.toJMonth = None
        self.evalMonths = None
        self.holdingMonths = None
        self.quantiles = None

        for attr in self.__dict__:
            self.__dict__[attr] = attr

class AllResults(RSSParams):
    def __init__(self):
        super().__init__()
        self.buy_and_hold = None
        self.rebalanced = None
        self.bh_cum = None
        self.rb_cum = None
        self.bh_desc = None
        self.rb_desc = None

        self.strategyType = None
        self.binNo = None
        self.meanReturns = None
        self.meanTStat = None
        self.meanPVal = None

        for attr in self.__dict__:
            self.__dict__[attr] = attr

class Portfolio(AdjPricesWithDates):
    def __init__(self):
        super().__init__()
        self.tradeDate = None

        for attr in self.__dict__:
            self.__dict__[attr] = attr

class JupyNBReports():
    def __init__(self):
        self.portfo_type = None
        self.short = None
        self.long = None
        self.longMinusShort = None

        for attr in self.__dict__:
            self.__dict__[attr] = attr  ##
