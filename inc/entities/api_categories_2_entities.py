import config
from inc import api_classes, api_properties
import csv
from os.path import join, realpath, exists
from os import makedirs

from util.util_categories import get_categories_from_file, handle_duplicate_results
from util.util import batched
from util import util_log

import config

global_path = config.ENTITIES_PATH
if not exists(global_path):
    makedirs(global_path)


async def save_tables(root_path, folder_name, common_description, props_dict):
    target_path = join(global_path, root_path, folder_name)
    if not exists(target_path):
        makedirs(target_path)

    for k, v in props_dict.items():
        rows = [['description', common_description]]
        rows.extend([[vi[0], vi[1]] for vi in v])
        with open(join(target_path, k + '.csv'), 'w', encoding='utf-8', newline='') as file:
            csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerows(rows)


async def group_by_prop(res_props):
    # here we merge duplicated props of interest
    final_dict = {}

    for k, v in res_props.items():
        unique_props = list(set(['{}_sol_{}'.format(item['propLabel'], item['p']) for item in v]))
        matrix = [[(p, '{}_sol_{}'.format(i['oLabel'], i['o']))
                   for i in v if '{}_sol_{}'.format(i['propLabel'], i['p']) == p]
                  for p in unique_props]

        for i in matrix:
            if len(i) > 1:
                final_value = ', '.join([j[1].split('_sol_')[0] for j in i])
                # aggregate back the _sol_ separator
                final_value += '_sol_' + ', '.join([j[1].split('_sol_')[1] for j in i])
            else:
                final_value = i[0][1]
            prop = i[0][0]
            # update the final_entities_dict with the new copy in all cases
            if k in final_dict:
                final_dict[k] += [(prop, final_value)]
            else:
                final_dict.update({k: [(prop, final_value)]})

    return final_dict


async def generate_recursive_entity_tables(categories, res_strs, depth=0):

    # pass these categories to get subclasses
    res_instances = await api_classes.get_instances(categories)

    res_instances = await handle_duplicate_results(res_instances, table_type_path='entities')

    new_categories = util_log.log_diff(
        message='api_categories_2_entities.py: Level {}: New items found via instances:'.format(depth),
        res_dict=res_instances)

    for k, v in res_instances.items():

        # capture common description for the current category
        cate_desc = ', '.join([vi['value'] for vi in res_strs[k] if 'description' in vi['prop']])

        instances = [vi['child'] for vi in v if vi['child'].startswith('Q')]

        if instances:

            res_props = {}
            for batch in batched(instances, 500):
                try:
                    res_props.update(await api_properties.get_wiki_props_for_lst(batch))
                except Exception as e:
                    print(e)

            # filter entities with less than 2 properties
            useless_keys = [k for k, v in res_props.items() if len(v) < 2]
            [res_props.pop(k) for k in useless_keys]

            # merge props with the same name into a comma separated field.
            merged_props_dict = await group_by_prop(res_props)

            # filter again after merging ... remove entities with less than 2 properties
            useless_keys = [k for k, v in merged_props_dict.items() if len(v) < 2]
            [merged_props_dict.pop(k) for k in useless_keys]

            await save_tables(root_path='instances', folder_name=k, common_description=cate_desc,
                              props_dict=merged_props_dict)

    # stopping condition, you reached max depth (configured) or nothing is retrieved
    depth += 1
    if depth == config.MAX_DEPTH or len(new_categories) == 0:
        return res_instances

    # retrieve new descriptions for the new categories
    new_res_strs = await api_properties.get_strings_for_lst(new_categories)

    # go deeper with the hierarchy
    await generate_recursive_entity_tables(new_categories, new_res_strs, depth)


async def parse_categories():
    # open data/Categories.csv parse it to get categories list
    categories = await get_categories_from_file()

    util_log.info('api_categories_2_entities.py: Categories found: {}'.format(len(categories)))

    # get categories descriptions
    res_strs = await api_properties.get_strings_for_lst(categories)

    # call the recursive function generates entities of entities
    res_instances = await generate_recursive_entity_tables(categories, res_strs)

    return res_instances
