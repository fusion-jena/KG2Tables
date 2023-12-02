from os import mkdir, listdir
from os.path import join, realpath, exists
import pandas as pd
import numpy as np


Results_Path = join(realpath('.'), 'Data_Explore_Results')
Data_Path = join(realpath('..'), 'data', 'results')

dataset_names = ['tbiodiv']

###############################################################

if not exists(Results_Path):
    mkdir(Results_Path)

def get_attributes(tables_path, table):
    tab_path = join(tables_path, table)
    df = pd.read_csv(tab_path, header=None)

    df_rows, df_cols = df.shape
    df_cells = df_cols * df_rows
    return df_rows, df_cols, df_cells

if __name__ == '__main__':
    for name in dataset_names:

        tables_path = join(Data_Path, name, 'horizontal', 'tables')
        tables = listdir(tables_path)
        rows, cols, cells = [], [], []
        print(f'{name}')
        print(f'Tables: {len(tables)}')

        for table in tables:
            tRows, tCols, tCells = get_attributes(tables_path, table)
            rows = rows + [tRows]
            cols = cols + [tCols]
            cells = cells + [tCells]

        df = pd.DataFrame(data={'rows':rows, 'cols': cols, 'cells': cells})
        df.to_csv(join(Results_Path, '{0}_statistics.csv'.format(name)))

        avgRows = np.average(rows)
        stdRows = np.std(rows)
        print('Rows:', avgRows, '+-', stdRows)

        avgCols = np.average(cols)
        stdCols = np.std(cols)
        print('Cols:', avgCols, '+-', stdCols)

        avgCells = np.average(cells)
        stdCells = np.std(cells)
        print('Cells:', avgCells, '+-', stdCells)

        #########################
        print('Targets ................')
        targets_path = join(Data_Path, name, 'horizontal', 'targets')
        targets = listdir(targets_path)
        for target in targets:
            df = pd.read_csv(join(targets_path, target), header=None)
            print(f'{target} : {len(df)}')
        print('==========================================')

