from os.path import join, realpath, exists
from config import CATEGORIES_FILE_NAME, MAX_NO_INSTANCES
import csv

cate_file_path = join(realpath('.'), 'data', 'input', CATEGORIES_FILE_NAME)


async def get_categories_from_file():
    cates = []
    with open(cate_file_path) as file:
        spamreader = csv.reader(file, delimiter=',', quotechar='|')
        for row in spamreader:
            if row[0]:
                cates += [row[0]]
    return list(set(cates))


async def trim_children(res_instances):
    """
    Sets max number to retrieved instances per category to config.MAX_NO_INSTANCES
    """
    trimmed_res = {}

    [trimmed_res.update({k: res_instances[k][0:min(len(res_instances[k]), MAX_NO_INSTANCES)]})
        for k in res_instances.keys()]

    return trimmed_res


async def handle_duplicate_results(res_instances, table_type_path='entities'):
    target_path = join(realpath('.'), 'data', 'results', table_type_path, 'processed_entities.txt')

    current_entities = []
    for k, v in res_instances.items():
        current_entities.extend([i['child'] for i in v])

    # load processed entities if existing
    processed = []
    if exists(target_path):
        with open(target_path, "r", encoding='utf8', newline='\n') as file:
            processed = file.read().splitlines()

    # dump new entities to the processed file
    new_entities = [new_entity for new_entity in current_entities if new_entity not in processed]
    with open(target_path, "a", encoding='utf8') as file:
        file.write('\n'.join(new_entities))

    # return a modified copy of the to_process dict keep items with new entities only
    new_res_instances = {}
    # initialize a new dict wit the same keys
    [new_res_instances.update({k: []}) for k in res_instances.keys()]

    for k, v in res_instances.items():
        for i in v:
            if i['child'] in new_entities:
                new_res_instances[k].append(i)

    return new_res_instances
