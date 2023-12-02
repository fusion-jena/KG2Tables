from os import mkdir, listdir
from os.path import join, realpath, exists
import pandas as pd
import numpy as np




Results_Path = join(realpath('.'), 'Data_Explore_Results')
Data_Path = join(realpath('.'), 'Data', '2023')

dataset_names = listdir(Data_Path)

###############################################################

if not exists(Results_Path):
    mkdir(Results_Path)

def get_attributes(dataset_name, split, table):
    tab_path = join(Data_Path, dataset_name, split, 'tables', table)
    df = pd.read_csv(tab_path, header=None)

    df_rows, df_cols = df.shape
    df_cells = df_cols * df_rows
    return df_rows, df_cols, df_cells

if __name__ == '__main__':
    for name in dataset_names:
        splits = listdir(join(Data_Path, name))
        for split in splits:
            if split.lower() == 'test':
                tables = listdir(join(Data_Path, name, split, 'tables'))
                rows, cols, cells = [], [], []
                print(f'{name} - {split}')
                print(f'Tables: {len(tables)}')

                for table in tables:
                    tRows, tCols, tCells = get_attributes(name, split, table)
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
                targets = listdir(join(Data_Path, name, split, 'targets'))
                for target in targets:
                    df = pd.read_csv(join(Data_Path, name, split, 'targets', target), header=None)
                    print(f'{target} : {len(df)}')
                print('==========================================')

