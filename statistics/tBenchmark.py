import os
from os import mkdir, listdir
from os.path import join, realpath, exists
import pandas as pd
import numpy as np

Results_Path = join(realpath('.'), 'Data_Explore_Results')
Data_Path = join(realpath('..'), 'data', 'results')

dataset_names = ['tFood', 'tfood10']

###############################################################

if not exists(Results_Path):
    mkdir(Results_Path)


def get_attributes(tables_path, table):
    tab_path = join(tables_path, table)
    df = pd.read_csv(tab_path, header=None)

    df_rows, df_cols = df.shape
    df_cells = df_cols * df_rows
    return df_rows, df_cols, df_cells


def get_counts():
    for name in dataset_names:
        size = 0
        dataset_path = join(Data_Path, name)
        tables_path = join(dataset_path, 'horizontal', 'tables')
        tables = listdir(tables_path)
        print(f'{name}')
        print(f'Horizontal Tables: {len(tables)}')

        entity_tables_path = join(dataset_path, 'entity', 'tables')
        etables = listdir(entity_tables_path)
        print(f'Entity Tables: {len(etables)}')

        print(f'Entity Tables: {len(tables) + len(etables)}')

        for ele in os.scandir(tables_path):
            size += os.path.getsize(ele)

        for ele in os.scandir(entity_tables_path):
            size += os.path.getsize(ele)
        print(f'Tables Disk Size: {size/(1024*1024)} MB')

        size = 0
        for path, dirs, files in os.walk(dataset_path):
            for f in files:
                fp = os.path.join(path, f)
                size += os.path.getsize(fp)
        print(f'Tables+gt+targets Disk Size: {size / (1024 * 1024)} MB')

def get_detailed_statistics():
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

        df = pd.DataFrame(data={'rows': rows, 'cols': cols, 'cells': cells})
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


if __name__ == '__main__':
    # get_detailed_statistics()
    get_counts()
