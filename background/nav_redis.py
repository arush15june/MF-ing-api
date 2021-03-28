"""Fetch mutual fund data and push it to a Redis cache for later use.
"""
import os
import asyncio
import dataclasses
from typing import List

import amfi
import redis
from redisearch import AutoCompleter, Suggestion

SCHEME_TYPE_PREFIX = 'SCHEME_TYPE'
SCHEME_SUB_TYPE_PREFIX = 'SCHEME_SUB_TYPE'
SCHEME_SUB_TYPE_FUND_HOUSE_PREFIX = 'SCHEME_SUB_TYPE_FUND_HOUSE'
FUND_HOUSE_PREFIX = 'FUND_HOUSE'
FUND_PREFIX = 'FUND'
PREFIX_DELIMTER = ':'

FUND_HOUSE_AUTOCOMPLETER_KEY = 'fund_house_ac'
FUND_AUTOCOMPLETER_KEY = 'fund_ac'
SCHEME_SUB_TYPE_AUTOCOMPLETER_KEY = 'scheme_sub_type_ac'
SCHEME_SUB_TYPE_FUND_HOUSE_AUTOCOMPLETER_KEY = 'scheme_sub_type_fund_house_ac'

class RedisClient:
    """Base class for creating Redis based stuff.
    """
    DEFAULT_HOST = os.getenv('REDIS_HOST', 'localhost')
    DEFAULT_PORT = os.getenv('REDIS_PORT', 6379)
    DEFAULT_DB = int(os.getenv('REDIS_DB', 0))

    def __init__(self, *args, **kwargs):
        """Connect to redis using kwargs or env vars.

        Keyword Arguments
        -----------------
        host: str
            Redis Host
        port: int
            Redis Port
        db: int
            Redis DB
        """
        host = kwargs.get('host', self.DEFAULT_HOST)
        port = kwargs.get('port', self.DEFAULT_PORT)
        db = kwargs.get('db', self.DEFAULT_DB)
        self.r = redis.Redis(host=host,
          port=port,
          db=db
        )

class FundKeyNotFoundError(Exception):
    """Raise error when fund name is not present in redis cache from AMFINavCache.get_fund. """
    def __init__(self, key, *args, **kwargs):
        self.key = key

    def __str__(self):
        return f'Invalid Fund Name: {self.key}'

class FundHouseKeyNotFoundError(Exception):
    """Raise error when fund house name is not present in redis cache from AMFINavCache.get_fund_hose. """
    def __init__(self, fund_key, *args, **kwargs):
        self.key = key

    def __str__(self):
        return f'Invalid Fund House Name: {self.key}'

replace_prefix = (lambda prefix, item: str(item).replace(f'{prefix}{PREFIX_DELIMTER}', ''))

class AMFINavCache(RedisClient):
    """Fetch mutual fund data from AMFI
    and store it in Redis.
    
    Notes
    -----
    - Redis Data
        1. List of scheme type with all scheme sub types as values.
        Key: <scheme_type>
        Value: List[scheme_sub_type]

        self._add_scheme_type

        2. List of scheme sub type with all fund houses under scheme as values.
        Key: <scheme_type>:<scheme_sub_type>
        Value: List[<fund_house_name>]

        3. List of fund houses and scheme_sub_type with all funds of the fund house in scheme sub type.
        It is the same structure as the fund house list (4) partitioned by scheme_sub_type.
        Key: <scheme_sub_type>:<fund_house_name>
        Value: List[<fund_scheme_name>]

        self._add_fund_house_under_sub_type

        4. List of fund houses with all funds of the fund
        Key: <fund_house_name>
        Value: List[<fund_scheme_name>]

        self._add_fund_house

        5. Fund with information of the fund.
        Key: <fund_scheme_name>
        Value: <serialized_fund_str>

        self._set_fund
    All redis structures are prefixed and delimited by :
    """
    AUTOCOMPLETE_WEIGHT = 1.0

    def __init__(self, *args, **kwargs):
        super(AMFINavCache, self).__init__(*args, **kwargs)
        self.fund_house_ac = AutoCompleter(FUND_HOUSE_AUTOCOMPLETER_KEY, conn=self.r)
        self.fund_ac =  AutoCompleter(FUND_AUTOCOMPLETER_KEY, conn=self.r)
        self.scheme_sub_type_ac = AutoCompleter(SCHEME_SUB_TYPE_AUTOCOMPLETER_KEY, conn=self.r)
        self.scheme_sub_type_fund_house_ac = AutoCompleter(SCHEME_SUB_TYPE_FUND_HOUSE_AUTOCOMPLETER_KEY, conn=self.r)

    async def _add_scheme_type(self, scheme_type: str, scheme_sub_types: List[str]):
        scheme_type_key = f"{SCHEME_TYPE_PREFIX}{PREFIX_DELIMTER}{scheme_type}"
        self.r.sadd(scheme_type_key, *scheme_sub_types)

    async def _add_scheme_sub_type(self, scheme_type: str, scheme_sub_type: str, fund_house_names: List[str]):
        scheme_sub_type_key = f"{SCHEME_SUB_TYPE_PREFIX}{PREFIX_DELIMTER}{scheme_type}{PREFIX_DELIMTER}{scheme_sub_type}"
        
        self.r.sadd(scheme_sub_type_key, *fund_house_names)
        self.scheme_sub_type_ac.add_suggestions(Suggestion(scheme_sub_type_key, self.AUTOCOMPLETE_WEIGHT))

    async def _add_fund_house_under_sub_type(self, scheme_sub_type: str, fund_house_name: str, fund_scheme_names: List[str]):
        scheme_sub_type_fund_house_key = f"{SCHEME_SUB_TYPE_FUND_HOUSE_PREFIX}{PREFIX_DELIMTER}{scheme_sub_type}{PREFIX_DELIMTER}{fund_house_name}"
        
        self.r.sadd(scheme_sub_type_fund_house_key, *fund_scheme_names)
        self.scheme_sub_type_fund_house_ac.add_suggestions(Suggestion(scheme_sub_type_fund_house_key, self.AUTOCOMPLETE_WEIGHT))

    async def _add_fund_house(self, fund_house_name: str, fund_scheme_names: List[str]):
        fund_house_key = f"{FUND_HOUSE_PREFIX}{PREFIX_DELIMTER}{fund_house_name}"
        
        self.r.sadd(fund_house_key, *fund_scheme_names)
        self.fund_house_ac.add_suggestions(Suggestion(fund_house_key, self.AUTOCOMPLETE_WEIGHT))

    async def _set_fund(self, fund: amfi.Fund):
        serialized = amfi.serialize_fund(fund)
        fund_key = f"{FUND_PREFIX}{PREFIX_DELIMTER}{fund.SchemeName}"

        self.r.set(fund_key, serialized)
        self.fund_ac.add_suggestions(Suggestion(fund_key, self.AUTOCOMPLETE_WEIGHT))

    async def _async_update_mf_cache(self):
        """Load mutual fund data from amfi.get_all_mfs
        and update redis cache data.
        """
        parsed_funds = amfi.get_all_mfs()
        redis_futures = list()
        
        for scheme_type_name, scheme_sub_type in parsed_funds.items():
            scheme_sub_type_list = list()
            for scheme_sub_type_name, fund_houses in scheme_sub_type.items():
                fund_house_list = list()
                for fund_house_name, funds in fund_houses.items():
                    fund_list = list()
                    for fund in funds:
                        fund.SchemeType = scheme_type_name
                        fund.SchemeSubType = scheme_sub_type_name
                        fund.SchemeFundHouse = fund_house_name
                        redis_futures.append(self._set_fund(fund))
                        fund_list.append(fund.SchemeName)

                    redis_futures.append(self._add_fund_house_under_sub_type(scheme_sub_type_name, fund_house_name, fund_list))
                    redis_futures.append(self._add_fund_house(fund_house_name, fund_list))
                    fund_house_list.append(fund_house_name)

                redis_futures.append(self._add_scheme_sub_type(scheme_type_name, scheme_sub_type_name, fund_house_list))
                scheme_sub_type_list.append(scheme_sub_type_name)

            redis_futures.append(self._add_scheme_type(scheme_type_name, scheme_sub_type_list))

        await asyncio.gather(*redis_futures)

    def update_mf_cache(self):
        asyncio.run(self._async_update_mf_cache())

    def _get_scalar(self, key: str) -> str:
        return self.r.get(key)

    def _get_set(self, key: str) -> List[str]:
        return self.r.smembers(key)

    async def get_fund(self, fund_name: str) -> amfi.Fund:
        """Fetch a fund using its exact name.
        """
        key_str = f'{FUND_PREFIX}{PREFIX_DELIMTER}{fund_name}'
        json_data = self._get_scalar(key_str)
        if json_data is None:
            raise FundKeyNotFoundError(fund_name)
        return amfi.deserialize_fund(json_data)
    
    async def get_fund_house(self, fund_house_name: str) -> List[str]:
        """Fetch the list of funds belonging to a certain fund house name.
        """
        key_str = f'{FUND_HOUSE_PREFIX}{PREFIX_DELIMTER}{fund_house_name}'
        fund_list = self._get_set(key_str)
        if fund_list is None:
            raise FundHouseKeyNotFoundError(fund_house_name)
        return fund_list
    
    def _get_prefix_cursor(self, prefix: str, cursor: int = 0, count: int = 0) -> List[str]:
        return self.r.scan(
            match=f'{prefix}{PREFIX_DELIMTER}*',
            cursor=cursor,
            count=count,
        )
    
    def _get_prefix_keys(self, prefix: str):
        return self.r.keys(f'{prefix}{PREFIX_DELIMTER}*')
    
    def get_fund_count(self):
        return len(self._get_prefix_keys(FUND_PREFIX))
    
    async def get_all_funds(self, *args, **kwargs) -> List[amfi.Fund]:
        fund_keys = self._get_prefix_keys(FUND_PREFIX, *args, **kwargs)
        return sorted([
                replace_prefix(FUND_PREFIX, item.decode()) 
                for item in fund_keys
            ])

if __name__ == '__main__':
    ts = time.perf_counter()
    amfi_redis = AMFINavCache()
    amfi_redis.update_mf_cache()
    print(time.perf_counter() - ts)
