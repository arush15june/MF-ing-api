# MF-ing-api

An API for parsing mutual fund data from AMFI. It parses the AMFI Daily NAV text file, stores the NAV in redis and indexes the fund houses and funds for searching via redisearch

## Installation

Requires docker and docker-compose

## TODO: API Docs

| Endpoint | Usage |
| - | - |
| /api/v1/funds | Fetch names of all funds |
| /api/v1/fund?key=<fund_name> | Fetch data for a single fund |
| /api/v1/fund_house | Fetch funds of a single fund house |
| /api/v1/search/{q_type} | Search across funds and fund houses |
