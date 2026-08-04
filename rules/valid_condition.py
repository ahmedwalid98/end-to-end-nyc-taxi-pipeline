from pyspark.sql import functions as F

def get_nyc_trips_condition():
    return (
        (F.col("VendorID").isNotNull()) &
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") >= 0) &
        (F.col("total_amount") >= 0) &
        (F.col("passenger_count").between(1, 8))
    )