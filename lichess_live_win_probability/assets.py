from dagster import AssetExecutionContext, Definitions
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dlt import pipeline, destinations, progress

from lichess_live_win_probability.dlt_sources.lichess import lichess_db


@dlt_assets(
    dlt_source=lichess_db("data/lichess_db_standard_rated_2024-06.pgn.zst"),
    dlt_pipeline=pipeline(
        pipeline_name="lichess",
        destination=destinations.filesystem("data"),
        dataset_name="lichess",
        progress=progress.log(600),
    ),
    name="lichess",
    group_name="lichess",
)
def lichess_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
