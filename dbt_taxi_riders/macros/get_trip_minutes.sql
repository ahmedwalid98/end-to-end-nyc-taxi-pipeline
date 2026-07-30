{% macro get_trip_minutes(pickupdate, dropoffdate) %}
  {{ dbt.datediff(pickupdate,dropoffdate, 'minute') }}
{% endmacro %}