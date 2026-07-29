{% macro get_vendo_name(VendorID) %}
  CASE WHEN {{ VendorID }} = 1 THEN 'Creative Mobile Technologies, LLC'
    WHEN {{ VendorID }} = 2 THEN 'Curb Mobility, LLC'
    WHEN {{ VendorID }} = 6 THEN 'Myle Technologies Inc'
    WHEN {{ VendorID }} = 7 THEN 'Helix'
    ELSE NULL
{% end macro %}

