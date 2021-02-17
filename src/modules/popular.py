from modules.elastic import Elastic
from modules.utils import write_json_file, get_current_time


def update_popular_stats(stream_type):
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
    df_channels = Elastic().get_df(
        "channels_index", ["publisher_title", "publisher_name", "thumbnail_url"]
    )
    df_channels["publisher_id"] = df_channels.index

    # Filter by genre
    df_streams = df_streams[df_streams.stream_type == stream_type]
    df_popular = df_streams.sort_values(by="view_count", ascending=False)

    df_top_discover = (
        df_popular.groupby(["publisher_id"], sort=False)
        .agg(
            view_count=("view_count", "sum"), discover_count=("discovered_at", "count")
        )
        .reset_index()
    )

    df_top = df_top_discover.sort_values(
        by=["discover_count", "view_count"], ascending=False
    )
    df_top_channels_discover = df_top.head(5)[["publisher_id"]]

    df_top_channels = df_top_channels_discover.merge(df_channels, on="publisher_id")

    stats = {
        "updated": get_current_time(),
        "data": {
            "tags": [],
            "genres": [],
            "channels": df_top_channels.to_dict(orient="records"),
        },
    }

    write_json_file(stats, f"stats/popular_stats_{stream_type}.json")
