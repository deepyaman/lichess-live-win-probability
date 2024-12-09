from dagster import AssetExecutionContext, AssetKey, SourceAsset
from dagster_embedded_elt.dlt import (
    DagsterDltResource,
    DagsterDltTranslator,
    dlt_assets,
)
from dlt import pipeline, destinations, progress
from dlt.extract.resource import DltResource

from lichess_live_win_probability.dlt_sources.lichess import lichess_db


class LichessDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_key(self, resource: DltResource) -> AssetKey:
        return AssetKey(["dlt", resource.source_name, resource.name])


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
    dagster_dlt_translator=LichessDagsterDltTranslator(),
)
def lichess_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


lichess_source_assets = [
    SourceAsset(key, group_name="lichess") for key in lichess_assets.dependency_keys
]
