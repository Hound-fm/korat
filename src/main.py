from modules.elastic import Elastic
from modules.latest import update_latest_stats


def run():
    stream_types = ["music", "podcast", "audiobook"]
    for type in stream_types:
        update_latest_stats(type)
