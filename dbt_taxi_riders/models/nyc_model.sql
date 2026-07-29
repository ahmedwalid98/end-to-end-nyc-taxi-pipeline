select * 
from {{ source('nyc', 'raw_nyc_trips') }}