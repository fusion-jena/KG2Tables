import os
from os.path import join
from quart import Quart
import traceback

import config
import inc.api
import inc.horizontal.api_categories
import inc.horizontal.api_categories_2_descriptions
import inc.entities.api_categories_2_entities
import inc.horizontal.api_categories_subclasses
import inc.horizontal.api_categories_subclasses_2_descriptions
import inc.entities.api_categories_subclasses_2_entities
import inc.api_categories_gt_manager
import inc.api_classes
import inc.api_types
import inc.api_redirects
import inc.disambiguation

from util import util_log, util_count, util_split, util_entity_split

util_log.init("wikidata2tables_generator.log")
util_log.info("Creating benchmark: {} ".format(config.BENCHMARK_NAME))
util_log.info("Categories are: {}".format(config.CATEGORIES_FILE_NAME))

app = Quart(__name__)
app.debug = False


@app.route('/test')
async def routeTest():
    # TODO: Add any API you want to test here!
    res = {}
    return res


async def generate_horizontal_tables():
    # save horizontal tables (WITHOUT) long descriptions (Props ONLY)
    util_log.info(message='[Horizontal][PROPS Only] Categories to instances and their instances')
    res = await inc.horizontal.api_categories.parse_categories()
    util_log.info(message='[Horizontal][PROPS Only] Categories to instances and their subclasses+instances')
    res = await inc.horizontal.api_categories_subclasses.parse_categories()

    os.rename(join(config.HORIZONTAL_PATH, 'processed_entities.txt'),
              join(config.HORIZONTAL_PATH, 'processed_entities_props_only.txt'))

    # save horizontal tables PROPS + long DESCRIPTIONS
    util_log.info(message='[Horizontal][Props+Des] Categories to instances and their instances')
    inc.horizontal.api_categories.include_categories_desc = True
    res = await inc.horizontal.api_categories.parse_categories()
    util_log.info(message='[Horizontal][Props+Des] Categories to instances and their subclasses+instances')
    inc.horizontal.api_categories_subclasses.include_categories_desc = True
    res = await inc.horizontal.api_categories_subclasses.parse_categories()

    os.rename(join(config.HORIZONTAL_PATH, 'processed_entities.txt'),
              join(config.HORIZONTAL_PATH, 'processed_entities_props+descriptions.txt'))

    # save horizontal DESCRIPTIONS only
    util_log.info(message='[Horizontal][DESC Only] Categories to instances and their instances')
    res = await inc.horizontal.api_categories_2_descriptions.parse_categories()
    util_log.info(message='[Horizontal][DESC Only] Categories to instances and their subclasses+instances')
    res = await inc.horizontal.api_categories_subclasses_2_descriptions.parse_categories()

    os.rename(join(config.HORIZONTAL_PATH, 'processed_entities.txt'),
              join(config.HORIZONTAL_PATH, 'processed_entities_descriptions_only.txt'))

    return res


async def generate_entity_tables():
    """
    Generates the Entity Tables from given categories.csv
    """
    # save entity tables from instances and subclasses
    util_log.info(message='[Entities] Categories to instances and their instances')
    res = await inc.entities.api_categories_2_entities.parse_categories()
    util_log.info(message='[Entities] Categories to Subclasses and their subclasses+instances')
    res = await inc.entities.api_categories_subclasses_2_entities.parse_categories()
    return res


@app.route('/generate_tables', methods=['GET'])
async def routeGenerate_tables():
    await generate_entity_tables()
    await generate_horizontal_tables()
    return {'message': 'Success: Horizontal and Entities Tables are fetched.'}


@app.route('/anonymize_tables', methods=['GET'])
async def routeAnonymize_tables():
    """
    Apply SemTab format to the generated tables
    Separate  actual tables from `gt` and `targets`  + anonymize tables names
    """
    await inc.api_categories_gt_manager.anonymize_files()
    return {'Message': 'Success, Tables are anonmized'}


@app.route('/count', methods=['GET'])
async def routeCount():
    res = await util_count.count_everything()
    return res


@app.route('/val_test_split', methods=['GET'])
async def routValTestSplit():
    """
    val/test splits for each table type
    """
    # Horizontal tables split
    util_split.split()

    # entity tables split
    util_entity_split.split()

    return {'message': 'success, validation and test splits are created.'}


@app.route('/generate_benchmark_at_once', methods=['GET'])
async def routeGenerate_benchmark_at_once():
    """
    Main entry point to construct a domain-specific benchmark
    """

    # 1. Create the benchmark tables
    await routeGenerate_tables()

    # 2. Anonymize the file name (should be backward traceable)
    res = await routeAnonymize_tables()

    # 3. Provides some statistics about the generated dataset
    res = await routeCount()

    # 4. val/test split creation
    res = await routValTestSplit()

    res.update({'message': 'Success: benchmark has been created'})
    return res


# ~~~~~~~~~~~~~~~~~~~~~~ Default ~~~~~~~~~~~~~~~~~~~~~~
@app.errorhandler(500)
def handle_500(e):
    """output the internal error stack in case of unhandled exception"""
    try:
        raise e
    except:
        return traceback.format_exc(), 500


@app.route('/')
def routeRoot():
    return 'wikidata2tables.generator.svc'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5007)
