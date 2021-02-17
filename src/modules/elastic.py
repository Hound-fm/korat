import eland as ed
from dotenv import dotenv_values
from elasticsearch import helpers
from elasticsearch import Elasticsearch

# Elastic-search node config
config = dotenv_values(".env")


class Elastic:
    def __init__(self):
        self.client = Elasticsearch(
            [config["ELASTIC_NODE"]],
            http_auth=(config["ELASTIC_USER"], config["ELASTIC_PASSWORD"]),
            http_compress=True,
        )

    # Get data frame from index
    def get_df(self, index, columns=[]):
        proxy = None
        if columns:
            proxy = ed.DataFrame(self.client, es_index_pattern=index, columns=columns)
        else:
            proxy = ed.DataFrame(self.client, es_index_pattern=index)
        pandas_df = ed.eland_to_pandas(proxy)
        return pandas_df
