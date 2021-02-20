from modules.elastic import Elastic
from modules.utils import write_json_file, get_current_time


def update_tags_stats(stream_type):
    # Connecting to an Elasticsearch instance running on 'localhost:9200'
    df_streams = Elastic().get_df(
        "streams_index",
        [
            "stream_type",
            "genres",
            "tags",
            "publisher_id",
            "publisher_title",
            "discovered_at",
            "repost_count",
            "view_count",
        ],
    )

    # Filter by genre
    df_streams = df_streams[df_streams.stream_type == stream_type]

    # Tags
    df_tags = df_streams.explode("tags").dropna(axis="index")
    df_tags = (
        df_tags.groupby(["tags"], sort=False)
        .agg(frequency=("tags", "count"), reach=("publisher_id", "nunique"))
        .reset_index()
    )
    df_tags = df_tags.sort_values(by=["frequency"], ascending=False)

    # genres
    df_genres = df_streams.explode("genres").dropna(axis="index")
    df_genres = (
        df_genres.groupby(["genres"], sort=False)
        .agg(frequency=("genres", "count"), reach=("publisher_id", "nunique"))
        .reset_index()
    )
    df_genres = df_genres.sort_values(by=["frequency", "reach"], ascending=False)

    stats = {
        "tags": df_tags.to_dict("records"),
        "genres": df_genres.to_dict("records"),
    }

    write_json_file(stats, f"stats/tags_frequency_{stream_type}.json")
