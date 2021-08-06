##
from collections import OrderedDict
import importlib
from py import z_ns
import pandas as pd

operation_order = ("prepareFilledAdjPricesData", "findAllAndWorkingDays")

op_or = operation_order

def main():
    pass

    ##

    scripts = OrderedDict()
    for script in op_or:
        scripts[script] = importlib.import_module(f'py.{script}',
                                                  package = None)

    ##
    for scn, module in scripts.items():
        module.main()
        print(f'Sc. {scn}.py done.')

##


if __name__ == '__main__':
    print("main.py starts.")
    main()
    print(f"main.py done.")
