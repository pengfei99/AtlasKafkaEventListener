import json

from atlas_client.client import Atlas
from atlas_client.entity_management.hive.HiveColumnManager import HiveColumnManager
from atlas_client.entity_management.hive.HiveDBManager import HiveDBManager
from atlas_client.entity_management.hive.HiveTableManager import HiveTableManager


class HiveEventHandler:
    def __init__(self, atlas_client: Atlas):
        self.atlas_client = atlas_client
        self.hive_db_manager = HiveDBManager(atlas_client)
        self.hive_table_manager = HiveTableManager(atlas_client)
        self.hive_column_manager = HiveColumnManager(atlas_client)

    def handle_create_table_event(self, event_msg: str):
        table_event_metadata = json.loads(event_msg)
        table_name = table_event_metadata['tableName']
        db_name = table_event_metadata['dbName']
        owner = table_event_metadata['owner']
        create_time = table_event_metadata["createTime"]
        cols = table_event_metadata["sd"]["cols"]
        data_location = table_event_metadata["sd"]["location"]

        # step 1 create/update hive db
        # here cluster name is the name space of each user(e.g. KUBERNETES_NAMESPACE=user-pengfei)
        cluster_name = "user-pengfei"
        db_description = "database for my stock market"
        self.hive_db_manager.create_entity(db_name, cluster_name, db_description, owner=owner)

        # step 2 create/update table
        table_description = "favorite stock"
        db_qualified_name = f"{cluster_name}@{db_name}"
        self.hive_table_manager.create_entity(table_name, db_qualified_name, table_description, owner=owner,
                                              create_time=create_time)

        # step 3 creat columns
        for col in cols:
            col_name=col["name"]
            col_type=col["type"]
            self.hive_column_manager.create_entity("stock_name", "string", "pengfei.org@pengfei-stock.favorite",
                                                   "name of the stock")


    #   hive_db.create_entity("pengfei-stock", "pengfei.org", "database for my stock market")
    #     hive_table.create_entity("favorite", "pengfei.org@pengfei-stock", "favorite stock")
    #     hive_column.create_entity("stock_id", "int", "pengfei.org@pengfei-stock.favorite", "id of the stock")
    #     hive_column.create_entity("stock_name", "string", "pengfei.org@pengfei-stock.favorite", "name of the stock")
    #

    def create_hive_meta_entities(self):
        # local = False
        # # config for atlas client
        # atlas_prod_hostname = "https://atlas.lab.sspcloud.fr"
        # atlas_prod_port = 443
        # oidc_token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJhUHNCSzhYRC1od1gtMWJFbjdZZDRLS0tWS0hYRy03RHg3STZDaVZZWUtRIn0.eyJleHAiOjE2NDg0NTM2MDYsImlhdCI6MTY0Nzg0ODgxMCwiYXV0aF90aW1lIjoxNjQ3ODQ4ODA2LCJqdGkiOiIzZTE2ZmQxYi1hNjE4LTRjMmEtOGIxMC0zNTY4YmU4N2NkYWIiLCJpc3MiOiJodHRwczovL2F1dGgubGFiLnNzcGNsb3VkLmZyL2F1dGgvcmVhbG1zL3NzcGNsb3VkIiwiYXVkIjpbIm1pbmlvLWRhdGFub2RlIiwib255eGlhIiwiYWNjb3VudCJdLCJzdWIiOiI0NzM0OTEyOC00YTRjLTQyMjYtYTViMS02ODA4MDFhZjVhMmIiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJvbnl4aWEiLCJub25jZSI6IjkwMmIwMGFlLWY4ZjQtNDQxMS05MjQ5LTgwZWZiMTA5MjJlMCIsInNlc3Npb25fc3RhdGUiOiJkOWE2YWIxNC1mNDU1LTQ1ZWYtYmIzOS02OWFjYjdlYzk0MmUiLCJhY3IiOiIwIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIkF0bGFzX3JvbGVfYWRtaW4iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGdyb3VwcyBlbWFpbCIsInNpZCI6ImQ5YTZhYjE0LWY0NTUtNDVlZi1iYjM5LTY5YWNiN2VjOTQyZSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJuYW1lIjoiUGVuZ2ZlaSBMaXUiLCJncm91cHMiOlsiZm9ybWF0aW9uIiwicHJvamV0LW9ueXhpYSIsInJlbGV2YW5jIiwic3BhcmstbGFiIiwic3NwY2xvdWQtYWRtaW4iLCJ2dGwiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoicGVuZ2ZlaSIsImdpdmVuX25hbWUiOiJQZW5nZmVpIiwibG9jYWxlIjoiZnIiLCJmYW1pbHlfbmFtZSI6IkxpdSIsImVtYWlsIjoibGl1LnBlbmdmZWlAaG90bWFpbC5mciIsInBvbGljeSI6InN0c29ubHkifQ.SnmBX2ZwJsY-_rG-gKe4tWBXqoQzuwGPukAN2M529CS0QzKeiIB8N8zSvh4dwaMC1TsJvX49FKUD9ju7QJ7uQ7m6tdmxRfGXTsNbrK1HBCcWa5_8jUJmo-yuYDMxF_YDX92lCDpQEsZBlo9ChNVJg5tbkd3gXTwlGlmRcoemz7-a8D2pkAXVibBV9qM87TtJyQzebBoVQkiBZ5bjLXURuSDekpa6_2Iat2m2xP_pgEuZKm6pKbKpXsE-BiS0lh-Hz7A3Uey9FEVOnXmkpsTWNG1Ri9todLb8y9IEtJHupOzIIKtpB0ZIorbKPApUXxhmeZj4FXssLEcCZDb7BZekKw"
        # atlas_local_hostname = "http://localhost"
        # login = "admin"
        # pwd = "admin"

        # if local:
        #     atlas_client = Atlas(atlas_local_hostname, port=21000, username=login, password=pwd)
        # else:
        #     # create an instance of the atlas Client with oidc token
        #     atlas_client = Atlas(atlas_prod_hostname, atlas_prod_port, oidc_token=oidc_token)
        # finder = EntityFinder(atlas_client)
        # res = finder.search_full_text("hive_table", "pengfei")
        # print(f"Search result is {res}")

        # insert hive tables

        self.hive_table_manager.create_entity("favorite", "pengfei.org@pengfei-stock", "favorite stock")
        self.hive_column_manager.create_entity("stock_id", "int", "pengfei.org@pengfei-stock.favorite",
                                               "id of the stock")
        self.hive_column_manager.create_entity("stock_name", "string", "pengfei.org@pengfei-stock.favorite",
                                               "name of the stock")
