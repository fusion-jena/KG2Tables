import os
import shutil
from math import ceil
from os import listdir, makedirs
from os.path import join, realpath, exists, splitext
from random import seed, randint

import pandas as pd
import numpy as np

seed(42)

Results_Path = join(realpath('..'), 'data', 'results')
Data_Path = join(realpath('..'), 'data', 'results')

dataset_names = ['tFood', 'tfood10']

sample_size = 3  # this is percent value --> 1%


###############################################################


def get_random_table_names(tables_path, table_type, dest_path):
    tables = listdir(tables_path)
    print(f'Horizontal Tables: {len(tables)}')

    # calculate sample size
    dataset_sample_size = ceil((sample_size / 100) * len(tables))
    print(f'Generating Sample {table_type} Tables: {dataset_sample_size}')

    rand_indx = [randint(0, dataset_sample_size) for _ in range(dataset_sample_size)]

    sample_tables = [tables[i] for i in rand_indx]

    [shutil.copy2(src=join(tables_path, st), dst=dest_path) for st in sample_tables]

    sample_tables = [splitext(t)[0] for t in sample_tables]
    return sample_tables


table_types = ['horizontal', 'entity']


def sample_targets_gt(sample_tables, targets_path, dest_targets_path):
    targets = listdir(targets_path)

    for target in targets:
        df = pd.read_csv(join(targets_path, target), header=0)

        # Add header to the dataframe to enable a vectorized code for filteration
        cols = ['file'] + [f'col{i}' for i in range(df.shape[1] - 1)]
        df = df.set_axis(cols, axis=1)

        # actual filteration step
        df = df[df['file'].isin(sample_tables)]
        print(target)
        print(df.head(5))

        # save it to the destination targets
        df.to_csv(join(dest_targets_path, target), header=0, index=0)


# def sample_gt(sample_tables, gt_path, dest_gts_path):
#     gts = listdir(gt_path)
#
#     for gt in gts:
#         df = pd.read_csv(join(gt_path, gt), header=0)
#
#         cols = ['file'] + [f'col{i}' for i in range(df.shape[1] - 1)]
#         df = df.set_axis(cols, axis=1)
#
#         # actual filteration step
#         df = df[df['file'].isin(sample_tables)]
#         print(gt)
#         print(df.head(5))
#
#         df.to_csv(join(dest_gts_path, gt), header=0, index=0)


if __name__ == '__main__':
    for name in dataset_names:
        sample_dataset_name = f'{name}-{sample_size / 100}-sample'
        sample_dataset_path = join(Results_Path, sample_dataset_name)

        print(f'{name} --> {sample_dataset_name}')

        for table_type in table_types:
            dataset_path = join(Data_Path, name)
            tables_path = join(dataset_path, table_type, 'tables')
            targets_path = join(dataset_path, table_type, 'targets')
            gt_path = join(dataset_path, table_type, 'gt')

            dest_tables_path = join(sample_dataset_path, table_type, 'tables')
            dest_targets_path = join(sample_dataset_path, table_type, 'targets')
            dest_gt_path = join(sample_dataset_path, table_type, 'gt')

            if not exists(join(sample_dataset_path, table_type)):
                makedirs(dest_tables_path)
                makedirs(dest_targets_path)
                makedirs(dest_gt_path)

            sample_tables = get_random_table_names(tables_path, table_type, dest_tables_path)

            sample_targets_gt(sample_tables, targets_path, dest_targets_path)

            sample_targets_gt(sample_tables, gt_path, dest_gt_path)
    print('====================================')
