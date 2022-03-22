import json

from atlas_client.client import Atlas
from atlas_client.entity_management.hive.HiveColumnManager import HiveColumnManager
from atlas_client.entity_management.hive.HiveDBManager import HiveDBManager
from atlas_client.entity_management.hive.HiveTableManager import HiveTableManager
from atlas_client.entity_search.EntityFinder import EntityFinder


class HiveEventHandler:
    def __init__(self, atlas_client: Atlas):
        self.atlas_client = atlas_client
        self.hive_db_manager = HiveDBManager(atlas_client)
        self.hive_table_manager = HiveTableManager(atlas_client)
        self.hive_column_manager = HiveColumnManager(atlas_client)
        self.finder=EntityFinder(atlas_client)

    def handle_create_table_event(self, event_msg: str):
        table_event_metadata = json.loads(event_msg)
        table_name = table_event_metadata['tableName']
        db_name = table_event_metadata['dbName']
        owner = table_event_metadata['owner']
        create_time = table_event_metadata["createTime"]
        cols = table_event_metadata["sd"]["cols"]
        data_location = table_event_metadata["sd"]["location"]

        table_description = table_event_metadata["parameters"]["comment"]

        # step 1 create/update hive db
        # here cluster name is the name space of each user(e.g. KUBERNETES_NAMESPACE=user-pengfei)
        cluster_name = table_event_metadata["clusterName"]
        db_description = ""
        self.hive_db_manager.create_entity(db_name, cluster_name, db_description, owner=owner)

        # step 2 create/update table
        db_qualified_name = f"{cluster_name}@{db_name}"
        self.hive_table_manager.create_entity(table_name, db_qualified_name, table_description, owner=owner,
                                              create_time=create_time)

        # step 3 creat columns
        table_qualified_name = f"{db_qualified_name}.{table_name}"
        for col in cols:
            col_name = col["name"]
            col_type = col["type"]
            col_description = col["comment"]
            self.hive_column_manager.create_entity(col_name, col_type, table_qualified_name,
                                                   col_description)

    def handle_drop_table_event(self, event_msg: str):
        table_event_metadata = json.loads(event_msg)
        table_name = table_event_metadata['tableName']
        db_name = table_event_metadata['dbName']
        cluster_name = table_event_metadata["clusterName"]
        self.finder.search_full_text()

