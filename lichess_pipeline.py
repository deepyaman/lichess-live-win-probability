import io

import chess.pgn
import dlt
import pyarrow as pa
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

            yield pa.Table.from_pylist(
                [
                    {**game.headers, "move_ply": ply, "move_comment": move.comment}
                    for ply, move in enumerate(game.mainline(), start=1)
                ]
            )
            bar()


pipeline = dlt.pipeline(
    pipeline_name="lichess",
    destination=dlt.destinations.filesystem("data"),
    dataset_name="lichess",
)
pipeline.run(
    games("data/lichess_db_standard_rated_2024-06.pgn.zst"),
    loader_file_format="parquet",
)
