import asyncio
from .query import *
import inc.cache
from util.util import getWikiID

cacheSubclass = inc.cache.Cache('subclass', ['base'])
cacheInstances = inc.cache.Cache('instance', ['base'])
cacheSuperclass = inc.cache.Cache('superclass', ['base'])


async def get_subclasses(klasses):
    """retrieve all subclasses for the given one"""

    #res = await runQuerySingleKey(cacheSubclass, klasses, """
     #    SELECT ?base ?child
     #    WHERE {
     #      VALUES ?base { %s } .
     #      { ?child p:P31/ps:P31 ?base . }
     #      UNION
     #      { ?child p:P279/ps:P279 ?base . }
     #    }
     #  """, max_entity=5)

    res = await runQuerySingleKey(cacheSubclass, klasses, """
             SELECT ?base ?child
             WHERE {
               VALUES ?base { %s } .               
               ?child p:P279/ps:P279 ?base . 
             }
           """, max_entity=5)

    return res

async def get_instances(klasses):

    """retrieve all instances for the given one"""
    res = await runQuerySingleKey(cacheInstances, klasses, """
             SELECT ?base ?child
             WHERE {
               VALUES ?base { %s } .               
               ?child wdt:P31 ?base . 
             }
           """, max_entity=5)

    return res


async def get_parents_for_lst(klasses):
    """Get most generic type for a given entity mention P279 in Wikidata"""

    res = await runQuerySingleKey(cacheSuperclass, klasses, """
      SELECT ?base ?parent ?parentLabel
      WHERE {
        VALUES ?base { %s } .
        ?base wdt:P279 ?parent .
        SERVICE wikibase:label {bd:serviceParam wikibase:language "en".}
      }
    """)
    return res
