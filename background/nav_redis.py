"""Fetch mutual fund data and push it to a Redis cache for later use.
"""
import os, json, asyncio, dataclasses
from typing import List
import amfi, redis

class EnhancedJSONEncoder(json.JSONEncoder):

    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


class RedisClient:
    __doc__ = 'Base class for creating Redis based stuff.\n    '
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
          db=db)


class AMFINavCache(RedisClient):
    __doc__ = 'Fetch mutual fund data from AMFI\n    and store it in Redis.\n    '
    SCHEME_TYPE_PREFIX = 'SCHEME_TYPE'
    SCHEME_SUB_TYPE_PREFIX = 'SCHEME_SUB_TYPE'
    SCHEME_SUB_TYPE_FUND_HOUSE_PREFIX = 'SCHEME_SUB_TYPE_FUND_HOUSE'
    FUND_HOUSE_PREFIX = 'FUND_HOUSE'
    FUND_PREFIX = 'FUND'
    PREFIX_DELIMTER = ':'

    def __init__(self, *args, **kwargs):
        (super(AMFINavCache, self).__init__)(*args, **kwargs)

    @staticmethod
    def _serialize_fund(fund: amfi.Fund) -> str:
        """Serialize amfi.Fund to a str.

        Notes
        -----
            Format: json
            TODO: decide serialization format
        """
        return json.dumps(fund, cls=EnhancedJSONEncoder)

    @staticmethod
    def _deserialize_fund(serialized_fund: str) -> amfi.Fund:
        """Deserialize serialized fund from str to amfi.Fund.

        Notes
        -----
            Format: json
            TODO: decide serialization format
        """
        return amfi.Fund(**json.loads(serialized_fund))

    async def _add_scheme_type(self, scheme_type: str, scheme_sub_types: List[str]):
        (self.r.sadd)(f"{self.SCHEME_TYPE_PREFIX}{self.PREFIX_DELIMTER}{scheme_type}", *scheme_sub_types)

    async def _add_scheme_sub_type(self, scheme_type: str, scheme_sub_type: str, fund_house_names: List[str]):
        (self.r.sadd)(f"{self.SCHEME_SUB_TYPE_PREFIX}{self.PREFIX_DELIMTER}{scheme_type}-{scheme_sub_type}", *fund_house_names)

    async def _add_fund_house_under_sub_type(self, scheme_sub_type: str, fund_house_name: str, fund_scheme_names: List[str]):
        (self.r.sadd)(f"{self.SCHEME_SUB_TYPE_FUND_HOUSE_PREFIX}{self.PREFIX_DELIMTER}{scheme_sub_type}-{fund_house_name}", *fund_scheme_names)

    async def _add_fund_house(self, fund_house_name: str, fund_scheme_names: List[str]):
        (self.r.sadd)(f"{self.FUND_HOUSE_PREFIX}{self.PREFIX_DELIMTER}{fund_house_name}", *fund_scheme_names)

    async def _set_fund(self, fund: amfi.Fund):
        serialized = self._serialize_fund(fund)
        self.r.set(f"{self.FUND_PREFIX}{self.PREFIX_DELIMTER}{fund.SchemeName}", serialized)

    async def _async_update_mf_cache(self):
        """Load mutual fund data from amfi.get_all_mfs
        and update redis cache data.

        Notes
        -----
            - Redis Data
              1. List of scheme type with all scheme sub types as values.
                Key: <scheme_type>
                Value: List[scheme_sub_type]

                self._add_scheme_type

              2. List of scheme sub type with all fund houses under scheme as values.
                Key: <scheme_sub_type>
                Value: List[<fund_house_name>]

              3. List of fund houses and scheme_sub_type with all funds of the fund in scheme sub type
                Key: <scheme_sub_type>:<fund_house_name>
                Value: List[<fund_scheme_name>]

                self._add_fund_house_under_sub_type

              3. List of fund houses with all funds of the fund
                Key: <fund_house_name>
                Value: List[<fund_scheme_name>]

                self._add_fund_house

              4. Fund with information of the fund.
                Key: <fund_scheme_name>
                Value: <serialized_fund_str>

                self._set_fund
            All redis structures are prefixed and delimited by :
        """
        parsed_funds = amfi.get_all_mfs()
        redis_futures = list()
        for scheme_type, scheme_sub_type in parsed_funds.items():
            scheme_sub_type_list = list()
            for scheme_sub_type_name, fund_houses in scheme_sub_type.items():
                fund_house_list = list()
                for fund_house_name, funds in fund_houses.items():
                    fund_list = list()
                    for fund in funds:
                        redis_futures.append(self._set_fund(fund))
                        fund_list.append(fund.SchemeName)

                    redis_futures.append(self._add_fund_house_under_sub_type(scheme_sub_type_name, fund_house_name, fund_list))
                    redis_futures.append(self._add_fund_house(fund_house_name, fund_list))
                    fund_house_list.append(fund_house_name)

                redis_futures.append(self._add_scheme_sub_type(scheme_type, scheme_sub_type_name, fund_house_list))
                scheme_sub_type_list.append(scheme_sub_type_name)

            redis_futures.append(self._add_scheme_type(scheme_type, scheme_sub_type_list))

        await asyncio.gather(*redis_futures)

    def update_mf_cache(self):
        asyncio.run(self._async_update_mf_cache())


if __name__ == '__main__':
    ts = time.perf_counter()
    amfi_redis = AMFINavCache()
    amfi_redis.update_mf_cache()
    print(time.perf_counter() - ts)
