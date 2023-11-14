import re
from itertools import islice
import string, random

def get_random_letter(length):

    letters = string.ascii_uppercase
    result_str = ''.join(random.choice(letters) for _ in range(length))
    return result_str

def getWikiID(iri):
    """
      extract the ID from a wikidata IRI
      used to harmonize between the different namespaces
      """
    match = re.search(r'wikidata\.org.*[\/:]([QPL]\d+)', iri, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        return iri


def get_batch(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i + n]


def batched(iterable, n):
    "Batch data into lists of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch