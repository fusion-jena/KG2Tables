from os import mkdir, listdir
from os.path import join, realpath, exists
import pandas as pd
import numpy as np


# Results_Path = join(realpath('.'), 'Data_Explore_Results')
Data_Path = join(realpath('..'), 'zenodo', 'biodivtab_benchmark', 'tables')
targets_path = join(realpath('..'), 'zenodo', 'biodivtab_benchmark', 'targets')
###############################################################

# if not exists(Results_Path):
#     mkdir(Results_Path)

def get_attributes(table):
    tab_path = join(Data_Path, table)
    df = pd.read_csv(tab_path)

    df_rows, df_cols = df.shape
    df_cells = df_cols * df_rows
    return df_rows, df_cols, df_cells

if __name__ == '__main__':
    tables = listdir(Data_Path)
    rows, cols, cells = [], [], []
    for table in tables:
        tRows, tCols, tCells = get_attributes(table)
        rows = rows + [tRows]
        cols = cols + [tCols]
        cells = cells + [tCells]

    df = pd.DataFrame(data={'rows':rows, 'cols': cols, 'cells': cells})

    avgRows = np.average(rows)
    stdRows = np.std(rows)
    print('Rows:', avgRows, '+-', stdRows)

    avgCols = np.average(cols)
    stdCols = np.std(cols)
    print('Cols:', avgCols, '+-', stdCols)

    avgCells = np.average(cells)
    stdCells = np.std(cells)
    print('Cells:', avgCells, '+-', stdCells)

    # targets parsing
    cea_target_path = join(targets_path, 'CEA_biodivtab_2021_Targets.csv')
    df = pd.read_csv(cea_target_path, header=None)
    print('CEA: ', len(df))

    cta_target_path = join(targets_path, 'CTA_biodivtab_2021_Targets.csv')
    df = pd.read_csv(cta_target_path, header=None)
    print('CTA: ', len(df))



