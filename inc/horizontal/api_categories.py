import xmlrpc.client
from functools import reduce
import asyncio

import config
from inc import api_classes, api_properties
import csv
from os.path import join, realpath, exists
from os import makedirs
from collections import OrderedDict

import pandas as pd

from itertools import islice, zip_longest

from util import util_log
from util.util_categories import get_categories_from_file, handle_duplicate_results, trim_children
from util.util import batched
import random

global_path = config.HORIZONTAL_PATH

include_categories_desc = False


async def get_common_props(parentCateName, res_props, prop_dict):
    for Qxx, v in res_props.items():
        ps = list(set([vi['propLabel'] for vi in v]))
        [prop_dict[pi].append(Qxx) if pi in prop_dict.keys() else prop_dict.update({pi: [Qxx]}) for pi in ps]
        # print(prop_dict)


async def get_overlaps(index_dict, common_entities):
    res = []
    for start in range(len(index_dict)):
        # save a table with all entites with partial intersection we can pass a range
        for ln in range(start + 2, len(index_dict)):
            # for ln in range(start + 2, min(len(index_dict), 5)):
            common_ls = list(reduce(lambda i, j: i & j, (set(x) for x in common_entities[start:ln])))
            if not common_ls:
                break
            if len(common_ls) > 1:
                # print(common_ls)
                # extract corresponding props from index_dict value
                corr_props = [index_dict[k][0] for k in range(start, ln)]
                # print(corr_props)
                # print('------------------------')
                res.append([common_ls, corr_props])
    return res


async def get_full_entities(shared_entities, their_props, res_props):
    full_entities_dict = {}
    for k, v in res_props.items():
        if k in shared_entities:
            full_entities_dict.update({k: v})

    # keep props of intrest only
    full_ents_new_dict = {}
    for k, v in full_entities_dict.items():
        full_ents_new_dict.update({k: [i for i in v if i['propLabel'] in their_props]})

    del full_entities_dict

    # here we merge duplicated props of interest
    final_entities_dict = {}
    # merge list of the same prop to be comma separated
    for p in their_props:
        for k, v in full_ents_new_dict.items():

            # this should represent all items with the same prop with a comma separated field.
            items_with_same_p = [i for i in v if i['propLabel'] == p]
            if len(items_with_same_p) < 1:
                row_copy = {'p': '', 'o': '', 'propLabel': p, 'pLabel': '', 'oLabel': ''}
            else:
                row_copy = items_with_same_p[0]
            if len(items_with_same_p) > 1:
                # update the new copy by merging the oLabel value  (convert from multi items to comma separted field)
                row_copy['oLabel'] = ', '.join([i['oLabel'] for i in items_with_same_p])
                row_copy['o'] = ', '.join([i['o'] for i in items_with_same_p])

            # update the final_entities_dict with the new copy in all cases
            if k in final_entities_dict:
                final_entities_dict[k] += [row_copy]
            else:
                final_entities_dict.update({k: [row_copy]})

    # delete the unused var
    del full_ents_new_dict

    return final_entities_dict


async def save_random_props(root_path, df, filename, common_description=None):
    if common_description:
        root_path += '+descriptions'

    target_path = join(global_path, 'instances', root_path)
    if not exists(target_path):
        makedirs(target_path)

    cols = list(df.columns)
    cnt = len(cols)

    random.seed(42)
    cols_to_remove = random.sample(range(1, cnt), int(0.5 * cnt))
    final_cols = [cols[i] for i in range(cnt) if i not in cols_to_remove]

    new_df = df[final_cols]

    new_df.to_csv(join(target_path, filename), sep=',', encoding='utf-8', index=False, header=True)


async def save_table(root_path, filename, cols, rows, common_description=None):
    if common_description:
        root_path += '+descriptions'

    target_path = join(global_path, 'instances', root_path)
    if not exists(target_path):
        makedirs(target_path)

    df = pd.DataFrame()
    df['gt_R2I'] = list(rows.keys())

    for c in cols:
        cValues = []
        p = ''
        for k, v in rows.items():
            cValues.extend(['{}_sol_{}'.format(i['oLabel'], i['o']) for i in v if i['propLabel'] == c])
            if p == '':
                p = [i['p'] for i in v if i['propLabel'] == c][0]
        df['{}_sol_{}'.format(c, p)] = cValues

    if common_description:
        # generate random number for 40% of the rows have the common description
        cnt = len(rows.keys())
        random.seed(42)
        indx_with_desc = random.sample(range(0, cnt), int(0.5 * cnt))
        df['description'] = [common_description if i in indx_with_desc else "" for i in range(cnt)]

    print(df.head(2))
    # if  hasHeader:
    df.to_csv(join(target_path, filename), sep=',', encoding='utf-8', index=False, header=True)
    return df
    # else:
    #   anony_header = ['col{}'.format(i) for i in range(len(cols))]
    #   columns = {}
    #   [columns.update({oldC:newC}) for oldC, newC in zip (cols, anony_header)]
    #   df.rename(columns=columns, inplace=True)
    #   df.to_csv(join(target_path, filename), sep=',', encoding='utf-8', index=False, columns=anony_header,
    #             header=True)


async def generate_recursive_horizontal_tables(categories, res_strs=None, depth=0):
    # pass these categories to get subclasses
    res_instances = await api_classes.get_instances(categories)

    # limit number of instances to configured value (Q5 to only e.g., 1000 )
    res_instances = await trim_children(res_instances)

    res_instances = await handle_duplicate_results(res_instances, table_type_path='horizontal')

    new_categories = util_log.log_diff(
        message='api_categories.py: '
                'Level {}: '
                'New items found via instances:'.format(depth),
        res_dict=res_instances)

    # the actual common description that would appear in tables
    cate_desc = None
    for k, v in res_instances.items():

        if include_categories_desc:
            # capture common description for the current category
            cate_desc = ', '.join([vi['value'] for vi in res_strs[k] if 'description' in vi['prop']])

        instances = [vi['child'] for vi in v if vi['child'].startswith('Q')]

        if instances:

            prop_dict = {}
            res_props = {}
            for batch in batched(instances, 500):
                try:
                    res_props.update(await api_properties.get_wiki_props_for_lst(batch))
                except Exception as e:
                    print(e)

            await get_common_props(k, res_props, prop_dict)

            od = OrderedDict(sorted(prop_dict.items(),
                                    key=lambda kv: len(kv[1]), reverse=True))

            # we should keep items with at least 2 entities in common
            od = {i: od[i] for i in od if len(od[i]) > 1}

            # override dict values from list of entities to list of (entity, prop)
            common_entities = list(od.values())
            index_dict = {}
            [index_dict.update({i: item}) for i, item in zip(range(len(od)), od.items())]

            if len(index_dict) > 0:
                # save a table with all these entities without any intersection /all  "common" props those with support > 2
                their_props = [i[0] for i in list(index_dict.values())]
                entities = list(set([item for sublist in [i[1] for i in index_dict.values()] for item in sublist]))
                rows_obj = await get_full_entities(entities, their_props, res_props)

                # save all properties version
                all_props_df = await save_table(root_path='all_props', filename='{}.csv'.format(k), cols=their_props,
                                                rows=rows_obj, common_description=cate_desc)

                # save randomly selected properties version
                await save_random_props(root_path='random_props', filename='{}.csv'.format(k),
                                        df=all_props_df, common_description=cate_desc)

            # this gets those entities with common properties
            res = await get_overlaps(index_dict, common_entities)

            # the above results consists of sub-groups all possible combinations between common properties
            i = 0
            for shared_entities, their_props in res:
                rows_obj = await get_full_entities(shared_entities, their_props, res_props)

                await save_table(root_path='common_props', filename='{}_{}.csv'.format(k, i), cols=their_props,
                                 rows=rows_obj, common_description=cate_desc)
                i += 1

    # stopping condition, you reached max depth (configured) or nothing is retrieved
    depth += 1
    if depth == config.MAX_DEPTH or len(new_categories) == 0:
        return res_instances

    new_res_strs = None
    if include_categories_desc:
        # retrieve new descriptions for the new categories
        new_res_strs = await api_properties.get_strings_for_lst(new_categories)

    # go deeper with the hierarchy
    await generate_recursive_horizontal_tables(new_categories, new_res_strs, depth)


async def parse_categories():
    # open data/Categories.csv parse it to get categories list
    categories = await get_categories_from_file()

    util_log.info('api_categories.py: Categories found: {}'.format(len(categories)))

    res_strs = None
    if include_categories_desc:
        # get categories descriptions
        res_strs = await api_properties.get_strings_for_lst(categories)

    # call the recursive function generates entities of entities
    res_instances = await generate_recursive_horizontal_tables(categories, res_strs)

    return res_instances
