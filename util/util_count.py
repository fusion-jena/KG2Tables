import os
from os.path import join, realpath, basename
from collections import OrderedDict
import json
import inc.api_properties


async def count_everything():
    # TODO: Move the count to statistics_util.py

    """
    Count the entire tree of the generated tables
    """

    target_path = join(realpath('.'), 'data', 'results', 'entities')

    cpt1 = sum([len(files) for r, d, files in os.walk(target_path)])
    print(cpt1)

    target_path = join(realpath('.'), 'data', 'results', 'horizontal')
    cpt2 = sum([len(files) for r, d, files in os.walk(target_path)])
    print(cpt2)

    print('entities: {}'.format(cpt1))
    print('other files: {}'.format(cpt2))
    print('total: {}'.format(cpt1 + cpt2))

    target_path = join(realpath('.'), 'data', 'results')
    cnt = 0
    for root, dirs, files in os.walk(target_path):
        if 'entities' in root and len(files) > 0:
            cnt += len(files)
            continue
        print(basename(root))
        if len(files) == 0:
            continue
        cnt += len(files)
        print(len(files))
        print('------------')

    return {'message': 'success',
            'Entity Tables': cpt1,
            'Horizontal Tables': cpt2,
            'Total': cpt1 + cpt2}
    return res


async def count_entities():
    # TODO: Move the count to statistics_util.py
    """
    Count Entity Tables
    """
    target_path = join(realpath('.'), 'data', 'results', 'entities')
    cnt_dict = {}
    entities = os.listdir(join(target_path, 'subclasses', 'subclasses'))

    res_strs = await inc.api_properties.get_strings_for_lst(entities)
    for k, v in res_strs.items():
        print(k)
        print(v)
        break
    for e in entities:
        i_cnt, s_cnt = 0, 0
        try:
            i_cnt = len(os.listdir(join(target_path, 'instances', e)))
        except FileNotFoundError:
            pass
        try:
            s_cnt = len(os.listdir(join(target_path, 'subclasses', 'subclasses', e)))
        except FileNotFoundError:
            pass

        lbl = [vi['value'] for vi in res_strs[e] if 'rdf-schema#label' in vi['prop']]

        cnt_dict.update({e: {'inst': i_cnt, 'subs': s_cnt, 'lbl': lbl}})

    d_descending = OrderedDict(sorted(cnt_dict.items(),
                                      key=lambda kv: kv[1]['subs'], reverse=True))

    with open(join(realpath('.'), 'data', 'results', 'entities_subs.json'), 'w') as file:
        json.dump(d_descending, file, indent=4)

    d_descending = OrderedDict(sorted(cnt_dict.items(),
                                      key=lambda kv: kv[1]['inst'], reverse=True))

    with open(join(realpath('.'), 'data', 'results', 'entities_insts.json'), 'w') as file:
        json.dump(d_descending, file, indent=4)

    return {'message': 'success'}
