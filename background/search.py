"""Implement searching abilities on AMFINavCache in Redis. 
"""
from typing import List

from redisearch import AutoCompleter

from .nav_redis import SCHEME_TYPE_PREFIX, SCHEME_SUB_TYPE_PREFIX, SCHEME_SUB_TYPE_FUND_HOUSE_PREFIX, FUND_HOUSE_PREFIX, FUND_PREFIX, PREFIX_DELIMTER
from .nav_redis import FUND_HOUSE_AUTOCOMPLETER_KEY, FUND_AUTOCOMPLETER_KEY, SCHEME_SUB_TYPE_AUTOCOMPLETER_KEY, SCHEME_SUB_TYPE_FUND_HOUSE_AUTOCOMPLETER_KEY
from .nav_redis import RedisClient, replace_prefix

import amfi

class InvalidQueryTypeError(Exception):
    """Raise error when search query type is invalid in AMFINavCacheSearchClient.search. """
    def __init__(self, q_type, *args, **kwargs):
        self.query_type = q_type

    def __str__(self):
        return f'Invalid Query Type: {self.query_type}'

class AMFINavCacheSearchClient(RedisClient):
    AC_TYPES = {
        'fund': {
            'prefix': FUND_PREFIX,
            'ac_key': FUND_AUTOCOMPLETER_KEY,
            'key_transform': (lambda item: replace_prefix(FUND_PREFIX, item)),
        },
        'fund_house': {
            'prefix': FUND_HOUSE_PREFIX,
            'ac_key': FUND_HOUSE_AUTOCOMPLETER_KEY,
            'key_transform': (lambda item: replace_prefix(FUND_HOUSE_PREFIX, item)),
        },
        'scheme_sub_type': {
            'prefix': SCHEME_SUB_TYPE_PREFIX,
            'ac_key': SCHEME_SUB_TYPE_AUTOCOMPLETER_KEY,
            'key_transform': (lambda item: replace_prefix(SCHEME_SUB_TYPE_PREFIX, item).split(PREFIX_DELIMTER)),
        },
        'scheme_sub_type_fund_house': {
            'prefix': SCHEME_SUB_TYPE_FUND_HOUSE_PREFIX,
            'ac_key': SCHEME_SUB_TYPE_FUND_HOUSE_AUTOCOMPLETER_KEY,
            'key_transform': (lambda item: replace_prefix(SCHEME_SUB_TYPE_FUND_HOUSE_PREFIX, item).split(PREFIX_DELIMTER))
        },
    }
    
    ENABLE_FUZZY = True
    
    def __init__(self, *args, **kwargs):
        super(AMFINavCacheSearchClient, self).__init__(*args, **kwargs)

    @staticmethod
    def _search(ac, query, *args, **kwargs) -> List[str]:
        return ac.get_suggestions(query, **kwargs)

    @staticmethod
    def _query(prefix, query) -> str:
        return f'{prefix}{PREFIX_DELIMTER}{query}'

    async def search(self, query_type: str, query: str) -> List[str]:
        """Search fund using fund autocompleter, 
        Returns funds with prefix removed.
        
        Returns
        -------
            List of fund autocomplete suggestions with prefixes removed.
        """
        try:
            assert(query_type in self.AC_TYPES)
            query_provider = self.AC_TYPES.get(query_type)
        except:
            raise InvalidQueryTypeError(query_type)
        
        query_with_prefix = self._query(query_provider.get('prefix'), query)
        ac = AutoCompleter(query_provider.get('ac_key'), conn=self.r)
        
        results = self._search(ac, query_with_prefix, fuzzy=self.ENABLE_FUZZY)
        
        return [
            query_provider.get('key_transform')(item)
            for item in results
        ]
