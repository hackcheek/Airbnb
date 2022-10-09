from .req import AsyncRequest


__all__ = ['url', 'params', 'AsyncRequest']


url = "https://public.opendatasoft.com/api/v2/catalog/datasets/air-bnb-listings/exports/json"

params = dict(
    limit=-1, # All data
    offset=0, # Start from 0
    timezone='UTC',
)

