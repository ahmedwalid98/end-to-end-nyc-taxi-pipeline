{% macro get_vendo_name(VendorID) %}
  CASE {{ VendorID }} 
    WHEN  1 THEN 'Creative Mobile Technologies, LLC'
    WHEN  2 THEN 'Curb Mobility, LLC'
    WHEN  6 THEN 'Myle Technologies Inc'
    WHEN  7 THEN 'Helix'
    ELSE NULL
  END
{% endmacro %}

