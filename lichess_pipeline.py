import io
from itertools import islice

import chess.pgn
import dlt
import pandas as pd
import zstandard
from alive_progress import alive_bar

LICHESS_URL = "https://lichess.org/"


def get_games_and_moves(path):
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


@dlt.resource
def games_and_moves(path):
    games_and_moves = get_games_and_moves(path)
    while True:
        games = []
        moves = []
        for game in islice(games_and_moves, 100_000):
            moves += game.pop("moves")
            games.append(game)

        if not games:
            break

        yield dlt.mark.with_table_name(pd.DataFrame.from_records(games), "games")
        yield dlt.mark.with_table_name(pd.DataFrame.from_records(moves), "moves")


pipeline = dlt.pipeline(
    pipeline_name="lichess",
    destination=dlt.destinations.filesystem("data"),
    dataset_name="lichess",
    progress=dlt.progress.log(600),
)
pipeline.run(
    games_and_moves("data/lichess_db_standard_rated_2024-06.pgn.zst"),
    loader_file_format="parquet",
)
