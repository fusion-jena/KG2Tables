import os

BENCHMARK_NAME = 'tbiodiv10'
CATEGORIES_FILE_NAME = 'biodiv_categories.csv'
MAX_DEPTH = 10
MAX_NO_INSTANCES = 1000  # trim retrieved instances to this MAX no . Q5 will have at most 2000 instances and subclasses!

##### Lookup API Config.
Target_Ontology = "http://wikidata.org/entity/"
MaxHits = 50  # up to 50 according to https://www.wikidata.org/w/api.php?action=help&modules=wbsearchentities
Default_Lang = 'en'

# API URL
LOOKUP_API = "https://www.wikidata.org/w/api.php"

##### Endpoint API config.
# shorten all URIs returned
SHORTEN_URIS = True

# endpoint to query
wikidata_SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'

# endpoint for cache server
# None for local, SQlite-based cache
CACHE_ENDPOINT = None
# credentials
CACHE_USERNAME = 'YourCacheUserName'
CACHE_PASSWORD = 'YourCachePassword'

# prefix for caching
# only used for endpoint
CACHE_PREFIX = 'wd_'

##### Shared config ...
# maximum requests in parallel
# prevents IP-bans from Wikidata
MAX_PARALLEL_REQUESTS = int(os.environ.get('MAX_PARALLEL_REQUESTS', 5))

# default back-off time on 429 in case server does not give any
# time in seconds
DEFAULT_DELAY = int(os.environ.get('DEFAULT_DELAY', 10))

# maximum number of retries, if we are not at fault
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', 5))

# paths
CUR_PATH = os.path.dirname(os.path.abspath(__file__))
if os.environ.get('DOCKERIZED', False):
    CACHE_PATH = os.path.join(CUR_PATH, 'cache')
    ASSET_PATH = os.path.join(CUR_PATH, 'assets')
else:
    CACHE_PATH = os.path.abspath(os.path.join(CUR_PATH, '..', '..', 'assets', 'data', 'cache', 'Wikidata_Proxy'))
    ASSET_PATH = os.path.abspath(os.path.join(CUR_PATH, '..', '..', 'assets', 'Wikidata_Proxy'))

# make sure all paths exist
if not os.path.exists(CACHE_PATH):
    os.makedirs(CACHE_PATH)

# create required paths if not exists
RESULTS_PATH = os.path.join(os.path.realpath('.'), 'data', 'results')
ENTITIES_PATH = os.path.join(RESULTS_PATH, 'entities')
if not os.path.exists(ENTITIES_PATH):
    os.makedirs(ENTITIES_PATH)

HORIZONTAL_PATH = os.path.join(RESULTS_PATH, 'horizontal')
if not os.path.exists(HORIZONTAL_PATH):
    os.makedirs(HORIZONTAL_PATH)