 select distinct
        vendor_id,
        vendor_name
from {{ ref('raw_nyc_trips') }}