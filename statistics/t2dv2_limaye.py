from os.path import join, realpath
from os import listdir
import pandas as pd
import json
import numpy as np

def t2dv2_satistics():
    t2dv2_path = join(realpath('.'), 'data', 't2dv2')
    names = listdir(t2dv2_path)
    rows, cols, cells = [], [], []
    for name in names:
        with open(join(t2dv2_path, name), 'r', encoding='utf8', errors='ignore') as file:
            table = json.load(file)
            tcols = len(table['relation'])
            trows = len(table['relation'][0])
            tcells = tcols * trows

            rows += [trows]
            cols += [tcols]
            cells += [tcells]

    avgRows = np.average(rows)
    stdRows = np.std(rows)
    print('Rows:', avgRows, '+-', stdRows)

    avgCols = np.average(cols)
    stdCols = np.std(cols)
    print('Cols:', avgCols, '+-', stdCols)

    avgCells = np.average(cells)
    stdCells = np.std(cells)
    print('Cells:', avgCells, '+-', stdCells)

def limaye_statistics():
    t2dv2_path = join(realpath('.'), 'data', 'limaye')
    names = listdir(t2dv2_path)
    rows, cols, cells = [], [], []
    for name in names:
        table = pd.read_csv(join(t2dv2_path, name))
        tcols_ls = table.count().to_list()
        tcols_ls = [c for c in tcols_ls if c > 0]
        tcols = len(tcols_ls)

        if tcols:
            trows = tcols_ls[0]
            tcells = tcols * trows

            rows += [trows]
            cols += [tcols]
            cells += [tcells]
        else:
            print("empty table {}".format(name))

    avgRows = np.average(rows)
    stdRows = np.std(rows)
    print('Rows:', avgRows, '+-', stdRows)

    avgCols = np.average(cols)
    stdCols = np.std(cols)
    print('Cols:', avgCols, '+-', stdCols)

    avgCells = np.average(cells)
    stdCells = np.std(cells)
    print('Cells:', avgCells, '+-', stdCells)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    print('\nLimaye Statistics: ')
    limaye_statistics()
    print('\nT2Dv2 Statistics')
    t2dv2_satistics()
