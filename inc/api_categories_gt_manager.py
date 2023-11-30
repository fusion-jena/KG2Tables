import json
from math import floor
from os.path import join, exists, realpath, basename, splitext
from os import makedirs, walk, listdir
import pandas as pd
import random
import csv
import inc.api_types
from collections import Counter
from util import util_log
import re
import config

random.seed(42)
from util.util import get_random_letter, getWikiID

WIKI_ENT_ID = 'http://www.wikidata.org/entity/'
WIKI_P_ID = 'http://www.wikidata.org/prop/direct/'

input_path = join(realpath('.'), 'data', 'results')
tfood_path = join(realpath('.'), 'data', config.BENCHMARK_NAME)

entity_tables_path = join(tfood_path, 'entity', 'tables')
entity_gt_path = join(tfood_path, 'entity', 'gt')
entity_targets_path = join(tfood_path, 'entity', 'targets')

horizontal_tables_path = join(tfood_path, 'horizontal', 'tables')
horizontal_gt_path = join(tfood_path, 'horizontal', 'gt')
horizontal_targets_path = join(tfood_path, 'horizontal', 'targets')

# create necessary paths at a glance
paths = [entity_gt_path, entity_targets_path, entity_tables_path,
         horizontal_tables_path, horizontal_gt_path, horizontal_targets_path]
[makedirs(p) for p in paths if not exists(p)]


async def get_class_from_cell_annotations(cells_annotations):
    unique_cell_annotations = list(set(cells_annotations))
    short_annotations = [getWikiID(a) for a in cells_annotations]

    res_types = await inc.api_types.get_direct_types_for_lst(cells_annotations)
    col_types = []

    [col_types.extend([item['type'] for item in res_types[annotation]]) for annotation in short_annotations
                                if annotation in res_types]
    # col_types = [item['type'] for k, v in res_types.items() for item in v]

    od = Counter(col_types)

    # include all types with more than or equal 75% of the cell annotations
    # cnt = len(unique_cell_annotations)
    # return all types that has more than 50% support by unique cells
    column_classes = [WIKI_ENT_ID + x for x, count in od.items() if count >= floor(0.4 * len(cells_annotations))]
    if len(column_classes) == 0 and len(unique_cell_annotations) > 0:
        column_classes += ['NIL']
        print("NIL CTA")
    if len(column_classes) > 1:
        print("Various valid types")
    return ','.join(column_classes)


async def save_table(src_root, original_file, new_file):
    original_df = pd.read_csv(join(src_root, original_file), sep=',', header=0)
    original_df.fillna('?', inplace=True)

    # initialize ground truth lists
    td_gt, cea_gt, cta_gt, cpa_gt, r2i_gt = [], [], [], [], []

    # capture solution of TD
    filename = splitext(original_file)[0]
    classname = filename.split('_')[0]
    td_gt.append([splitext(new_file)[0], WIKI_ENT_ID + classname])

    original_df['row_count'] = pd.Series()
    # count how many cells are non-empty per row and append in a temp column = row_count
    # sum() in the lambda expression plays the role of count() it, here it counts the empty cells
    for i in range(original_df.shape[0]):  # iterate over rows
        real_cell_cnt = 0
        for j in range(original_df.shape[1]):  # iterate over columns
            try:
                if original_df.iloc[i,j] != '_sol_':
                    real_cell_cnt += 1
            except Exception as e:
                print(e)
        original_df['row_count'][i] = real_cell_cnt

    # original_df['row_count'] = original_df.apply(lambda x: (str(x) != '_sol_').sum(), axis=1)
    print(original_df.head(3))

    # get R2I gt by iterating over rows
    [r2i_gt.append([splitext(new_file)[0], i, WIKI_ENT_ID + original_df['gt_R2I'][i]])
     if original_df['row_count'][i] >= 3  # rows with at least 2 cell values can have 2ri, we put 3 since gt_R2I exits
     else r2i_gt.append([splitext(new_file)[0], i, 'NIL'])
     for i in range(original_df.shape[0])]

    # remove gt_R2I and temp column with row_count
    df = original_df.drop(['gt_R2I', 'row_count'], axis=1)
    # print(df.head(3))

    # add artificial subject_column ->  Rand 4 chars + sequence
    cell_value = get_random_letter(4)
    subject_col = ['{}{}'.format(cell_value, i) for i in range(df.shape[0])]
    df.insert(loc=0, column='subject_col', value=subject_col)

    # get CPA gt by iterating over columns
    for i, c in enumerate(df.columns):
        parts = c.split('_sol_')
        if len(parts) == 2:
            # zero maps to the artificially added subject_column
            cpa_gt.append([splitext(new_file)[0], 0, i, WIKI_P_ID + parts[1]])

    for j in range(df.shape[1]):  # iterate over columns
        cells_annotations = []
        for i in range(df.shape[0]):  # iterate over rows
            try:
                parts = (df.iloc[i,j]).split('_sol_')
                df.iloc[i, j] = parts[0]
                # TODO: change starts with to regex for WIKIID, this has http://wiki.../Queller in gt data
                #  I fixed it manually

                if len(parts) == 2 and parts[1].startswith('Q'):

                    # skip literal values no gt for CEA here
                    if parts[0] == parts[1]:
                        continue

                    regex = r'(Q)([\d]+)'
                    matches = re.findall(regex, parts[1])
                    if not matches:
                        continue  # skip any value that doesn't match this regex

                    # add it to targets of CEA (filename, column, row, gt)
                    gt_value = ','.join([WIKI_ENT_ID + v.strip() for v in parts[1].split(',')])
                    cea_gt += [[splitext(new_file)[0], j, i, gt_value]]

                    # add current cell annotation to the column annotations
                    cells_annotations += gt_value.split(',')
            except Exception as e:
                print(e)

        try:
            # process CTA gt
            column_classes = await get_class_from_cell_annotations(cells_annotations)
            if column_classes:
                cta_gt += [[splitext(new_file)[0], j, column_classes]]
        except Exception as e:
            print(e)

    anony_header = ['col{}'.format(i) for i in range(len(df.columns))]
    columns = {}
    [columns.update({oldC: newC}) for oldC, newC in zip(df.columns, anony_header)]

    df.rename(columns=columns, inplace=True)

    df.to_csv(join(horizontal_tables_path, new_file), header=True, columns=anony_header, index=False)

    return cea_gt, cta_gt, cpa_gt, r2i_gt, td_gt


async def save_entity_table(src_root, original_file, new_file):
    original_df = pd.read_csv(join(src_root, original_file), sep=',', header=None, names=['props', 'values'])
    original_df.fillna('')

    # initialize ground truth lists
    td_gt, cea_gt, cpa_gt = [], [], []

    # capture solution of TD
    filename = splitext(original_file)[0]
    classname = filename.split('_')[0]
    td_gt.append([splitext(new_file)[0], WIKI_ENT_ID + classname])

    # create a fresh copy (Deep copy has own copy of data and index.)
    df = original_df.copy(deep=True)

    # hide property names
    anony_props = ['{}{}'.format('Prop', i) for i in range(df.shape[0])]
    df.insert(loc=0, column='anony_props', value=anony_props)

    # get CPA gt by iterating over columns
    for i, c in enumerate(df['props']):
        parts = c.split('_sol_')
        if len(parts) == 2:
            # filename, row_id, gt
            cpa_gt.append([splitext(new_file)[0], i, WIKI_P_ID + parts[1]])

    # remove original props
    df.drop('props', axis=1, inplace=True)

    for j in range(1, df.shape[1]):  # iterate over columns but, skip first column/anony_props

        for i in range(df.shape[0]):  # iterate over rows
            try:
                parts = (df.iloc[i,j]).split('_sol_')
                df.iloc[i,j] = parts[0]
                if len(parts) == 2 and parts[1].startswith('Q'):
                    # skip literal values no gt for CEA here
                    if parts[0] == parts[1]:
                        continue

                    # add it to targets of CEA (filename, column, row, gt)
                    gt_value = ','.join([WIKI_ENT_ID + v.strip() for v in parts[1].split(',')])
                    cea_gt += [[splitext(new_file)[0], j, i, gt_value]]
            except Exception as e:
                print(e)

    df.to_csv(join(entity_tables_path, new_file), header=False, index=False)

    return cea_gt, cpa_gt, td_gt


async def save_gt_targets(dest_gt_path, dest_targets_path,
                          all_cea_gt, all_cta_gt, all_cpa_gt, all_r2i_gt, all_td_gt):
    # ----------------- cea ----------------------------
    with open(join(dest_gt_path, 'cea_gt.csv'), 'a', encoding='utf-8', newline='') as file:
        csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerows(all_cea_gt)

    all_cea_targets = [item[:-1] for item in all_cea_gt]
    with open(join(dest_targets_path, 'cea_targets.csv'), 'a', newline='') as file:
        csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerows(all_cea_targets)

    # ----------------- cta ----------------------------
    if all_cta_gt:
        with open(join(dest_gt_path, 'cta_gt.csv'), 'a', encoding='utf-8', newline='') as file:
            csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerows(all_cta_gt)

        all_cta_targets = [item[:-1] for item in all_cta_gt]
        with open(join(dest_targets_path, 'cta_targets.csv'), 'a', newline='') as file:
            csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerows(all_cta_targets)

    # ----------------- cpa ----------------------------
    with open(join(dest_gt_path, 'cpa_gt.csv'), 'a', encoding='utf-8', newline='') as file:
        csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerows(all_cpa_gt)

    all_cpa_targets = [item[:-1] for item in all_cpa_gt]
    with open(join(dest_targets_path, 'cpa_targets.csv'), 'a', newline='') as file:
        csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerows(all_cpa_targets)

    # ----------------- r2i ----------------------------
    if all_r2i_gt: #r2i has been changed to ra (Row Annotation) on 23 Nov 23
        with open(join(dest_gt_path, 'ra_gt.csv'), 'a', encoding='utf-8', newline='') as file:
            csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerows(all_r2i_gt)

        all_r2i_targets = [item[:-1] for item in all_r2i_gt]
        with open(join(dest_targets_path, 'ra_targets.csv'), 'a', newline='') as file:
            csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerows(all_r2i_targets)

    # ----------------- td ----------------------------
    with open(join(dest_gt_path, 'td_gt.csv'), 'a', encoding='utf-8', newline='') as file:
        csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerows(all_td_gt)

    all_td_targets = [item[:-1] for item in all_td_gt]
    with open(join(dest_targets_path, 'td_targets.csv'), 'a', newline='') as file:
        csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerows(all_td_targets)


async def get_table_categories_map(target_path):
    all_table_cates = ['horizontal', 'entities']
    [all_table_cates.extend(dirs) for _, dirs, _ in walk(target_path)]
    all_table_cates = sorted(list(set(all_table_cates)))
    table_cates_map = {}
    [table_cates_map.update({c: i}) for i, c in enumerate(all_table_cates)]

    return table_cates_map


async def handel_entities_tables():
    entities_path = join(input_path, 'entities')

    for root, dirs, files in walk(entities_path):

        filename = await get_base_filename(root, entities_path)

        valid_files = [f for f in files if splitext(f)[1] == '.csv']
        cnt = len(valid_files)
        if cnt > 0:
            util_log.info(root)

            for i, f in enumerate(valid_files):

                util_log.info("{} is mapped to {}".format(f, i))

                current_final_name = filename + "I" + str(i).zfill(len(str(cnt))) + '.csv'

                # actual saving of the file
                t_cea_gt, t_cpa_gt, t_td_gt = await save_entity_table(src_root=root,
                                                                      original_file=f, new_file=current_final_name)

                # save gt and targets
                await save_gt_targets(entity_gt_path, entity_targets_path,
                                      t_cea_gt, None, t_cpa_gt, None, t_td_gt)


async def get_base_filename(root, root_path):
    table_cates_map = await get_table_categories_map(root_path)

    util_log.info('Table categories map: %s' % json.dumps(table_cates_map))

    filename = get_random_letter(3)

    root_categories = root.split('\\')
    for cat in root_categories:
        if cat in table_cates_map:
            filename += str(table_cates_map[cat]).zfill(len(str(len(table_cates_map))))
            # filename += '_' # easier to debug

    return filename


async def handle_horizontal_tables():
    horizontal_path = join(input_path, 'horizontal')

    for root, dirs, files in walk(horizontal_path):

        filename = await get_base_filename(root, horizontal_path)

        valid_files = [f for f in files if splitext(f)[1] == '.csv']
        valid_files.sort()
        cnt = len(valid_files)
        if cnt > 0:
            util_log.info(root)

            for i, f in enumerate(valid_files):

                util_log.info("{} is mapped to {}".format(f, i))

                c_final_name = filename + "I" + str(i).zfill(len(str(cnt))) + '.csv'

                # actual saving of the file
                t_cea_gt, t_cta_gt, t_cpa_gt, t_r2i_gt, t_td_gt = await save_table(src_root=root, original_file=f,
                                                                                   new_file=c_final_name)

                # save gt and targets
                await save_gt_targets(horizontal_gt_path, horizontal_targets_path,
                                      t_cea_gt, t_cta_gt, t_cpa_gt, t_r2i_gt, t_td_gt)


async def anonymize_files():
    # transform original data to semtab format (tables/targets/gt)
    await handle_horizontal_tables()
    await handel_entities_tables()
