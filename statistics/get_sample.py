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

dataset_names = ['tfood10']

sample_size = 3  # this is percent value --> 1%

chunksize = 10000  # How many lines to be processed at the same time - lazy loading paramter


###############################################################


def get_random_table_names(tables_path, table_type, dest_path):
    tables = listdir(tables_path)
    print(f'{table_type} Tables: {len(tables)}')

    # calculate sample size
    dataset_sample_size = ceil((sample_size / 100) * len(tables))
    print(f'Generating Sample {table_type} Tables: {dataset_sample_size}')

    # rand_indx = [randint(0, dataset_sample_size) for _ in range(dataset_sample_size)]
    rand_indx = []
    for _ in range(len(tables) * 1000):
        new_rand = randint(0, len(tables))
        if new_rand not in rand_indx:
            rand_indx.append(new_rand)
        if len(rand_indx) == dataset_sample_size:
            break

    sample_tables = [tables[i] for i in rand_indx]

    [shutil.copy(src=join(tables_path, st), dst=dest_path) for st in sample_tables]

    sample_tables = [splitext(t)[0] for t in sample_tables]
    return sample_tables


table_types = ['horizontal', 'entity']


def sample_targets_gt(sample_tables, targets_path, dest_targets_path):
    targets = listdir(targets_path)

    for target in targets:
        cnt = 0
        total_lines = 0
        unique_entries = []
        # Lazy loading with pandas to handle huge files e.g., cea_targets and gt data.
        with pd.read_csv(join(targets_path, target), chunksize=chunksize, header=None) as reader:
            for df in reader:

                # Add header to the dataframe to enable a vectorized code for filteration
                cols = ['file'] + [f'col{i}' for i in range(df.shape[1] - 1)]
                df = df.set_axis(cols, axis=1)

                total_lines += len(df)
                unique_entries.extend(df['file'])
                unique_entries = list(set(unique_entries))

                # actual filteration step
                df = df[df['file'].isin(sample_tables)]

                # save it to the destination targets with append mode
                df.to_csv(join(dest_targets_path, target), mode='a', header=None, index=None)
                cnt += len(df)
        print(f'Appending Lines: {cnt} / {total_lines} to: {target} - Unique entries: {len(unique_entries)}')
    print('=============================================')


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
