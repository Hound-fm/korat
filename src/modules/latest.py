from modules.elastic import Elastic
from modules.utils import write_json_file, get_current_time


def update_latest_stats(stream_type):
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
        ],
    )
    df_channels = Elastic().get_df(
        "channels_index", ["publisher_title", "publisher_name", "thumbnail_url"]
    )
    df_channels["publisher_id"] = df_channels.index

    # Filter by genre
    df_streams = df_streams[df_streams.stream_type == stream_type]

    # ...
    df_oldest = df_streams.sort_values(by="discovered_at")
    df_latests = df_streams.sort_values(by="discovered_at", ascending=False)

    df_first_discover = df_oldest.groupby(
        "publisher_id", as_index=False, sort=False
    ).first()

    df_recent = df_first_discover.sort_values(by="discovered_at", ascending=False)
    df_recent_channel_discover = df_recent.head(5)[["publisher_id", "discovered_at"]]
    df_recent_channels = df_recent_channel_discover.merge(
        df_channels, on="publisher_id"
    )
    df_recent_channels.discovered_at = df_recent_channels.discovered_at.astype(str)

    df_recent_genres = df_latests.explode("genres").dropna(axis="index")
    df_recent_genres = df_recent_genres.groupby(
        "genres", as_index=False, sort=False
    ).first()

    df_recent_tags = df_latests.explode("tags").dropna(axis="index")
    df_recent_tags = df_recent_tags.groupby("tags", as_index=False, sort=False).first()

    recent_tags = df_recent_tags.head(5).tags.tolist()
    recent_genres = df_recent_genres.head(5).genres.tolist()

    stats = {
        "updated": get_current_time(),
        "data": {
            "tags": recent_tags,
            "genres": recent_genres,
            "channels": df_recent_channels.to_dict(orient="records"),
        },
    }
    write_json_file(stats, f"stats/latest_stats_{stream_type}.json")
