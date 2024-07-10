import io

import chess.pgn
import dlt
import zstandard
from alive_progress import alive_bar

LICHESS_URL = "https://lichess.org/"


@dlt.resource
def games(path):
    with open(path, "rb") as fh, alive_bar(89_342_529) as bar:
        dctx = zstandard.ZstdDecompressor()
        stream_reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(stream_reader, encoding="utf-8")
        while (game := chess.pgn.read_game(text_stream)) is not None:
            site = game.headers["Site"]
            if not site.startswith(LICHESS_URL):
                raise ValueError(f"Site must start with {LICHESS_URL}")

            game_id = site[len(LICHESS_URL) :]
            yield {
                "game_id": game_id,
                **game.headers,
                "moves": [
                    {"game_id": game_id, "ply": ply, "comment": move.comment}
                    for ply, move in enumerate(game.mainline(), start=1)
                ],
            }
            bar()


pipeline = dlt.pipeline(
    import_schema_path="schemas/import",
    export_schema_path="schemas/export",
    pipeline_name="lichess",
    destination="duckdb",
    dataset_name="main",
)
pipeline.run(
    games("data/lichess_db_standard_rated_2024-06.pgn.zst"),
    loader_file_format="parquet",
)
