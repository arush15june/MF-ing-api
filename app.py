from typing import Optional, List
import traceback

from fastapi import FastAPI, HTTPException, APIRouter, Query
from pydantic import BaseModel

from background import AMFINavCache, AMFINavCacheSearchClient, InvalidQueryTypeError, FundKeyNotFoundError, FundHouseKeyNotFoundError
import dataclasses

app = FastAPI()

nav_cache_provider = AMFINavCache()
nav_cache_search_provider = AMFINavCacheSearchClient()

class SearchResponse(BaseModel):
    q: Optional[str]
    results: Optional[List] = []

@app.get("/api/v1/search/{q_type}")
async def search_nav_cache(q_type: str, q: Optional[str] = None):
    """Search AMFINavCache using AMFINavCacheSearchClient.
    
    Notes
    -----
        Available types are defined in AMFINavCacheSearchClient.AC_TYPES
    """
    
    """TODO: Allow searching for everything. """
    if q_type not in ['fund', 'fund_house']:
        raise HTTPException(status_code=400, detail='Invalid query type.')
    
    try:
        results = await nav_cache_search_provider.search(q_type, q)
    except InvalidQueryTypeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    return SearchResponse(q=q, results=results)

class FetchRequest(BaseModel):
    key: str

class FundResponse(BaseModel):
    SchemeCode: str
    SchemeName: str
    ISINDivPayoutGrowth: str
    ISINDivReinvestment: str
    NAV: str
    Date: str

PAGE_COUNT_LIMIT = 1000

@app.get("/api/v1/fund")
async def fetch_all_funds(pg: Optional[int] = 0, count: Optional[int] = Query(10, le=PAGE_COUNT_LIMIT)):
    """Fetch all fund names.
    """
    try:
        fund_keys = await nav_cache_provider.get_all_funds()
        total_pages = int(len(fund_keys) / count)

        fund_keys = fund_keys[pg*count:pg*count+count]
    except Exception as e:
        raise HTTPException(status_code=400, detail='Error fetching funds.')

    return {
        'pg': pg,
        'items': fund_keys,
        'last': total_pages,
    }

@app.post("/api/v1/fund")
async def fetch_fund(item: FetchRequest) -> FundResponse:
    """Fetch fund with name: key.
    """
    try:
        fund = await nav_cache_provider.get_fund(item.key)
    except FundKeyNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return FundResponse(**dataclasses.asdict(fund))

@app.post("/api/v1/fund_house")
async def fetch_fund(item: FetchRequest, scheme_sub_type) -> List[str]:
    """Fetch list of funds in fund house with name: key.
    """
    try:
        funds = await nav_cache_provider.get_fund_house(item.key)
    except FundHouseKeyNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return list(funds)
