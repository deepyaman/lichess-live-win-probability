import io
from itertools import chain, islice

import chess.pgn
import dlt
import pyarrow as pa
import zstandard
from alive_progress import alive_bar

LICHESS_URL = "https://lichess.org/"
SCHEMA = pa.schema(
    [
        ("Event", pa.string()),
        ("Site", pa.string()),
        ("Date", pa.string()),
        ("Round", pa.string()),
        ("White", pa.string()),
        ("Black", pa.string()),
        ("Result", pa.string()),
        ("UTCDate", pa.string()),
        ("UTCTime", pa.string()),
        ("WhiteElo", pa.string()),
        ("BlackElo", pa.string()),
        ("WhiteRatingDiff", pa.string()),
        ("BlackRatingDiff", pa.string()),
        ("WhiteTitle", pa.string()),
        ("BlackTitle", pa.string()),
        ("ECO", pa.string()),
        ("Opening", pa.string()),
        ("TimeControl", pa.string()),
        ("Termination", pa.string()),
        ("move_ply", pa.int64()),
        ("move_comment", pa.string()),
    ]
)


def get_moves(path):
    with open(path, "rb") as fh, alive_bar(89_342_529) as bar:
        dctx = zstandard.ZstdDecompressor()
        stream_reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(stream_reader, encoding="utf-8")
        while (game := chess.pgn.read_game(text_stream)) is not None:
            site = game.headers["Site"]
            if not site.startswith(LICHESS_URL):
                raise ValueError(f"Site must start with {LICHESS_URL}")

            yield (
                {**game.headers, "move_ply": ply, "move_comment": move.comment}
                for ply, move in enumerate(game.mainline(), start=1)
            )
            bar()


@dlt.resource
def moves(path):
    moves = get_moves(path)
    while chunk := list(chain.from_iterable(islice(moves, 100_000))):
        yield pa.Table.from_pylist(chunk, schema=SCHEMA)


pipeline = dlt.pipeline(
    pipeline_name="lichess",
    destination=dlt.destinations.filesystem("data"),
    dataset_name="lichess",
    progress=dlt.progress.log(600),
)
pipeline.run(
    moves("data/lichess_db_standard_rated_2024-06.pgn.zst"),
    loader_file_format="parquet",
)
