from modules.elastic import Elastic
from modules.tags import update_tags_stats
from modules.latest import update_latest_stats
from modules.popular import update_popular_stats


def run():
    stream_types = ["music", "podcast", "audiobook"]
    for type in stream_types:
        update_tags_stats(type)
        update_latest_stats(type)
        update_popular_stats(type)
