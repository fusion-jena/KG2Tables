from os import mkdir, listdir, walk
from os.path import join, realpath, exists, basename
import pandas as pd
import numpy as np

Results_Path = join(realpath('.'), 'Data_Explore_Results')
Data_Path = join(realpath('.'), 'Data', 'tfood')

dataset_names = listdir(Data_Path)

###############################################################

if not exists(Results_Path):
    mkdir(Results_Path)

def get_most_frequent(lst):
    if len(lst) == 0:
        return []

    sorted_dict = weighted_sort(lst)
    # get highest val
    most_freq_key = next(iter(sorted_dict))
    most_freq_val = sorted_dict[most_freq_key]

    res = []
    # get ties if exists
    for key, val in sorted_dict.items():
        if val == most_freq_val:
            res = res + [key]
    return res

def weighted_sort(lst):
    unique_elems = list(set(lst))
    freq_dict = {}
    for uelem in unique_elems:
        freq_dict[uelem] = 0
        for elem in lst:
            if elem == uelem:
                freq_dict[uelem] = freq_dict[uelem] + 1

    sorted_dict = {}
    for w in sorted(freq_dict, key=freq_dict.get, reverse=True):
        sorted_dict[w] = freq_dict[w]

    return sorted_dict

def manual_parse(path):
    """manually parse csv file contents, if pandas failed"""

    with open(path, 'r', encoding='utf8') as file:
        lines = file.read().split('\n')
    cells = [line.strip().split(',') for line in lines]
    cnt = [len(line_cells) for line_cells in cells]
    cols_num = max(get_most_frequent(cnt))
    if cols_num == 1:
        cnt = [c for c in cnt if c != 1]
        cols_num = max(get_most_frequent(cnt))

    cols = []
    for i in range(cols_num):
        col = []
        for row in cells:
            try:
                col = col + [row[i]]
            except:
                col = col + [""]
        cols = cols + [col]
    no_rows = len(cols[0])
    no_cols = len(cols)
    no_cells = no_rows * no_cols
    return no_rows, no_cols, no_cells

def get_attributes(root, table):
    try:
        tab_path = join(root, table)
        df = pd.read_csv(tab_path, header=None, low_memory=False
                         , sep=',')

        df_rows, df_cols = df.shape
        df_cells = df_cols * df_rows
        return df_rows, df_cols, df_cells
    except Exception as e:
        return manual_parse(join(root, table))


if __name__ == '__main__':
    for name in dataset_names:
        rows, cols, cells = [], [], []
        target_dict = {}

        target_path = join(Data_Path, name)

        cnt = 0
        for root, dirs, files in walk(target_path):
            if not files or len(files) == 0:
                continue
            if 'tables' == basename(root):
                cnt += len(files)

                for table in files:
                    try:
                        tRows, tCols, tCells = get_attributes(root, table)
                        rows += [tRows]
                        cols += [tCols]
                        cells += [tCells]
                    except TypeError as e:
                        print(e)
                        # break

            if 'targets' == basename(root):
                supported_targets = listdir(root)

                for target in supported_targets:
                    try:
                        df = pd.read_csv(join(root, target), header=None, low_memory=False, names=['file'])
                        target_dict.update({target.lower(): target_dict.get(target.lower(), 0) + len(df)})
                    except Exception as e:
                        print(target)

        print('===========================================')
        print(name)
        print(cnt)

        if rows:
            avgRows = np.average(rows)
            stdRows = np.std(rows)
            print('Rows:', avgRows, '+-', stdRows)

            avgCols = np.average(cols)
            stdCols = np.std(cols)
            print('Cols:', avgCols, '+-', stdCols)

            avgCells = np.average(cells)
            stdCells = np.std(cells)
            print('Cells:', avgCells, '+-', stdCells)

            print('------------')
            for k, v in target_dict.items():
                print(k)
                print(v)

