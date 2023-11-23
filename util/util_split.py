from os import listdir, makedirs
from os.path import join, realpath, exists
import pandas as pd
from random import shuffle, seed
import shutil
import config

def split():
    table_categoreis = ['horizontal']

    for tc in table_categoreis:
        data_path = join(realpath('.'), 'data', config.BENCHMARK_NAME, tc)
        val_path = join(realpath('.'), 'data', '{}-splits'.format(config.BENCHMARK_NAME), tc, 'val')
        test_path = join(realpath('.'), 'data', '{}-splits'.format(config.BENCHMARK_NAME), tc, 'test')

        if not exists(val_path):
            makedirs(join(val_path, 'gt'))
            makedirs(join(val_path, 'targets'))
            makedirs(join(val_path, 'tables'))
        if not exists(test_path):
            makedirs(join(test_path, 'gt'))
            makedirs(join(test_path, 'targets'))
            makedirs(join(test_path, 'tables'))

        # split tables
        tables_path = join(data_path, 'tables')
        tables = listdir(tables_path)
        tables = [t.replace('.csv', '') for t in tables]
        tables_cnt = len(tables)

        seed(42)
        shuffle(tables)
        val_set = tables[0:int(0.1 * tables_cnt)]
        print(val_set)

        # target columns to drop
        cta_gt = pd.read_csv(join(data_path, 'gt', 'cta_gt.csv'), names=['file', 'colid', 'sol'])
        print(len(cta_gt))

        cta_gt = cta_gt[cta_gt['sol'].notna()]
        print(len(cta_gt))

        cea_gt = pd.read_csv(join(data_path, 'gt', 'cea_gt.csv'), names=['file', 'colid', 'rowid', 'sol'])
        print(len(cea_gt))
        cea_gt = cea_gt[cea_gt['sol'].notna()]
        print(len(cea_gt))

        cpa_gt = pd.read_csv(join(data_path, 'gt', 'cpa_gt.csv'), names=['file', 'subjid', 'objid', 'sol'])
        print(len(cpa_gt))
        cpa_gt = cpa_gt[cpa_gt['sol'].notna()]
        print(len(cpa_gt))

        r2i_gt = pd.read_csv(join(data_path, 'gt', 'ra_gt.csv'), names=['file', 'rowid', 'sol'])
        print(len(r2i_gt))
        r2i_gt = r2i_gt[r2i_gt['sol'].notna()]
        print(len(r2i_gt))

        td_gt = pd.read_csv(join(data_path, 'gt', 'td_gt.csv'), names=['file', 'sol'])
        print(len(td_gt))
        td_gt = td_gt[td_gt['sol'].notna()]
        print(len(td_gt))

        # pick gt for val &  test
        val_cta_gt = cta_gt.loc[cta_gt['file'].isin(val_set)]
        val_cea_gt = cea_gt.loc[cea_gt['file'].isin(val_set)]
        val_cpa_gt = cpa_gt.loc[cpa_gt['file'].isin(val_set)]
        val_r2i_gt = r2i_gt.loc[r2i_gt['file'].isin(val_set)]
        val_td_gt = td_gt.loc[td_gt['file'].isin(val_set)]

        test_cta_gt = cta_gt.loc[~cta_gt['file'].isin(val_set)]
        test_cea_gt = cea_gt.loc[~cea_gt['file'].isin(val_set)]
        test_cpa_gt = cpa_gt.loc[~cpa_gt['file'].isin(val_set)]
        test_r2i_gt = r2i_gt.loc[~r2i_gt['file'].isin(val_set)]
        test_td_gt = td_gt.loc[~td_gt['file'].isin(val_set)]

        # save gt data to val/test
        val_cta_gt.to_csv(join(val_path, 'gt', 'cta_gt.csv'), sep=',', index=False, header=False,
                          columns=['file', 'colid', 'sol'])
        val_cea_gt.to_csv(join(val_path, 'gt', 'cea_gt.csv'), sep=',', index=False, header=False,
                          columns=['file', 'colid', 'rowid', 'sol'])
        val_cpa_gt.to_csv(join(val_path, 'gt', 'cpa_gt.csv'), sep=',', index=False, header=False,
                          columns=['file', 'subjid', 'objid', 'sol'])
        val_r2i_gt.to_csv(join(val_path, 'gt', 'ra_gt.csv'), sep=',', index=False, header=False,
                          columns=['file', 'rowid', 'sol'])
        val_td_gt.to_csv(join(val_path, 'gt', 'td_gt.csv'), sep=',', index=False, header=False, columns=['file', 'sol'])

        test_cta_gt.to_csv(join(test_path, 'gt', 'cta_gt.csv'), sep=',', index=False, header=False,
                           columns=['file', 'colid', 'sol'])
        test_cea_gt.to_csv(join(test_path, 'gt', 'cea_gt.csv'), sep=',', index=False, header=False,
                           columns=['file', 'colid', 'rowid', 'sol'])
        test_cpa_gt.to_csv(join(test_path, 'gt', 'cpa_gt.csv'), sep=',', index=False, header=False,
                           columns=['file', 'subjid', 'objid', 'sol'])
        test_r2i_gt.to_csv(join(test_path, 'gt', 'ra_gt.csv'), sep=',', index=False, header=False,
                           columns=['file', 'rowid', 'sol'])
        test_td_gt.to_csv(join(test_path, 'gt', 'td_gt.csv'), sep=',', index=False, header=False,
                          columns=['file', 'sol'])

        # save targets to val/test
        val_cta_gt.to_csv(join(val_path, 'targets', 'cta_targets.csv'), sep=',', index=False, header=False,
                          columns=['file', 'colid'])
        val_cea_gt.to_csv(join(val_path, 'targets', 'cea_targets.csv'), sep=',', index=False, header=False,
                          columns=['file', 'colid', 'rowid'])
        val_cpa_gt.to_csv(join(val_path, 'targets', 'cpa_targets.csv'), sep=',', index=False, header=False,
                          columns=['file', 'subjid', 'objid'])
        val_r2i_gt.to_csv(join(val_path, 'targets', 'r2i_targets.csv'), sep=',', index=False, header=False,
                          columns=['file', 'rowid'])
        val_td_gt.to_csv(join(val_path, 'targets', 'td_targets.csv'), sep=',', index=False, header=False,
                         columns=['file'])

        test_cta_gt.to_csv(join(test_path, 'targets', 'cta_targets.csv'), sep=',', index=False, header=False,
                           columns=['file', 'colid'])
        test_cea_gt.to_csv(join(test_path, 'targets', 'cea_targets.csv'), sep=',', index=False, header=False,
                           columns=['file', 'colid', 'rowid'])
        test_cpa_gt.to_csv(join(test_path, 'targets', 'cpa_targets.csv'), sep=',', index=False, header=False,
                           columns=['file', 'subjid', 'objid'])
        test_r2i_gt.to_csv(join(test_path, 'targets', 'r2i_targets.csv'), sep=',', index=False, header=False,
                           columns=['file', 'rowid'])
        test_td_gt.to_csv(join(test_path, 'targets', 'td_targets.csv'), sep=',', index=False, header=False,
                          columns=['file'])

        # save tables to val/test
        [shutil.copyfile(join(tables_path, f"{t}.csv"), join(val_path, 'tables', f"{t}.csv")) for t in val_set]
        [shutil.copyfile(join(tables_path, f"{t}.csv"), join(test_path, 'tables', f"{t}.csv")) for t in tables if
         t not in val_set]

if __name__ == '__main__':
    split()