from modules.elastic import Elastic
from modules.utils import write_json_file, get_current_time


def get_usage_df(df, column):
    df_tags = df.explode(column).dropna(axis="index")
    df_tags = df_tags.rename(columns={f"{column}": "label"})
    df_tags = (
        df_tags.groupby(["label"], sort=False)
        .agg(frequency=("label", "count"), reach=("publisher_id", "nunique"))
        .reset_index()
    )
    df_tags = df_tags.sort_values(by=["frequency", "reach"], ascending=False)
    return df_tags


def update_tags_stats(stream_type):
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

    df_streams = df_streams[df_streams.stream_type == stream_type]

    stats = {
        "tags": get_usage_df(df_streams, "tags").to_dict("records"),
        "genres": get_usage_df(df_streams, "genres").to_dict("records"),
    }

    write_json_file(stats, f"stats/tags_frequency_{stream_type}.json")
