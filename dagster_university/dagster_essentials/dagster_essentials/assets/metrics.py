import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd

import duckdb
import os

from dagster_duckdb import DuckDBResource
from dagster_essentials.assets import constants
from dagster_essentials.partitions import weekly_partition

@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats(database: DuckDBResource) -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """
    
    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())


@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)


@dg.asset(deps=["taxi_trips"], partitions_def=weekly_partition)
def trips_by_week(context: dg.AssetExecutionContext, database: DuckDBResource)-> None:
    
    period_to_fetch = context.partition_key
    
    query = f"""
    COPY (
        select
            date_trunc('week', pickup_datetime) + interval '6 day' AS period,
            count(*) as num_trips,
            sum(passenger_count) AS passenger_count,
            sum(total_amount) AS total_amount,
            sum(trip_distance) AS trip_distance,
        from trips
        where pickup_datetime >= '{period_to_fetch}' and pickup_datetime < '{period_to_fetch}'::date + interval '1 week'
        group by date_trunc('week', pickup_datetime) + interval '6 day'
        ) TO '{constants.TRIPS_BY_WEEK_FILE_PATH}' (HEADER, DELIMITER ',')
    """
        
    with database.get_connection() as conn:
        conn.execute(query)