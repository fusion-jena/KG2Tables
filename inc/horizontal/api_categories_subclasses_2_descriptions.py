from inc import api_classes, api_properties
import csv
from os.path import join, realpath, exists
from os import makedirs
from util.util_categories import handle_duplicate_results, get_categories_from_file
import config

# results paths
from util.util import batched

descriptions_file_path = join(config.HORIZONTAL_PATH, 'subclasses', 'descriptions')


async def save_description_version(filename, res_strs, are_subclasses):
    if are_subclasses:
        # means outcome of this fn = tables from the subclasses of the categories
        folder_suffix = 'subclasses'
    else:
        # means: outcome of this fn = tables from instances of subclasses of the categories
        folder_suffix = 'instances'

    if not exists(join(descriptions_file_path, folder_suffix)):
        makedirs(join(descriptions_file_path, folder_suffix))

    lines = []
    for Qxx, v in res_strs.items():
        lines += [[Qxx, vi['value']] for vi in v if 'description' in vi['prop']]

    with open(join(descriptions_file_path, folder_suffix, filename + '.csv'), 'w'
            , encoding='utf8', errors='ignore', newline='') as file:
        csvwriter = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(['gt_R2I', 'description'])
        csvwriter.writerows(lines)


async def handle_results(res, are_subclasses=True):
    for k, v in res.items():
        instances = [vi['child'] for vi in v if vi['child'].startswith('Q')]

        if instances:
            res_strs = await api_properties.get_strings_for_lst(instances)
            await save_description_version(k, res_strs, are_subclasses)


async def parse_categories():
    # open data/Categories.csv parse it to get categories list
    categories = await get_categories_from_file()

    # pass these categories to get subclasses
    res_subclasses = await api_classes.get_subclasses(categories)

    res_subclasses = await handle_duplicate_results(res_subclasses, table_type_path='horizontal')

    # create tables that are derived from subclasses directly
    await handle_results(res_subclasses, are_subclasses=True)

    # the returned subclasses could be new categories, based on their number of instances
    new_categories = []
    for k, v in res_subclasses.items():
        new_categories += [vi['child'] for vi in v if vi['child'].startswith('Q')]

    # get instances of these subclasses
    new_instances = {}
    for batch in batched(new_categories, 500):
        try:
            new_instances.update(await api_classes.get_instances(batch))
        except Exception as e:
            print(e)

    print(len(new_instances))
    # We keep a category if it has more than 2 instances
    useless_cates = [k for k, v in new_instances.items() if len(v) < 2]
    [new_instances.pop(k) for k in useless_cates]
    print(len(new_instances))

    new_instances = await handle_duplicate_results(new_instances, table_type_path='')

    # handle tables that come from the new instances
    await handle_results(new_instances, are_subclasses=False)

    return res_subclasses
