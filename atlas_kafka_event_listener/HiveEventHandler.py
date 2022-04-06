import json

from atlas_client.client import Atlas
from atlas_client.entity_management.hive.HiveColumnManager import HiveColumnManager
from atlas_client.entity_management.hive.HiveDBManager import HiveDBManager
from atlas_client.entity_management.hive.HiveTableManager import HiveTableManager
from atlas_client.entity_search.EntityFinder import EntityFinder

from atlas_kafka_event_listener.LogManager import LogManager

my_logger = LogManager(__name__).get_logger()
my_logger.debug("Init HiveEventHandler")


class HiveEventHandler:
    def __init__(self, atlas_client: Atlas):
        self.atlas_client = atlas_client
        self.hive_db_manager = HiveDBManager(atlas_client)
        self.hive_table_manager = HiveTableManager(atlas_client)
        self.hive_column_manager = HiveColumnManager(atlas_client)
        self.finder = EntityFinder(atlas_client)

    def handle_create_table_event(self, event_msg: str):
        table_event_metadata = json.loads(event_msg)
        table_name = table_event_metadata.get('tableName').lower()
        db_name = table_event_metadata.get('dbName').lower()
        owner = table_event_metadata.get('owner')
        create_time = table_event_metadata.get("createTime")
        cols = table_event_metadata.get("sd").get("cols")
        data_location = table_event_metadata.get("sd").get("location")

        table_description = table_event_metadata.get("parameters").get("comment")

        # step 1 create/update hive db
        # here cluster name is the name space of each user(e.g. KUBERNETES_NAMESPACE=user-pengfei)
        cluster_name = table_event_metadata.get("clusterName")
        db_description = ""
        self.hive_db_manager.create_entity(db_name, cluster_name, db_description, owner=owner)

        # step 2 create/update table
        db_qualified_name = f"{cluster_name}@{db_name}"
        self.hive_table_manager.create_entity(table_name, db_qualified_name, table_description, owner=owner,
                                              create_time=create_time)

        # step 3 creat columns
        table_qualified_name = f"{db_qualified_name}.{table_name}"
        for col in cols:
            col_name = col.get("name").lower()
            col_type = col.get("type")
            col_description = col.get("comment")
            self.hive_column_manager.create_entity(col_name, col_type, table_qualified_name,
                                                   col_description)

    def handle_drop_table_event(self, event_msg: str, purge=False):
        table_event_metadata = json.loads(event_msg)
        table_name = table_event_metadata.get('tableName').lower()
        db_name = table_event_metadata.get('dbName').lower()
        cluster_name = table_event_metadata.get("clusterName")
        # build qualified name of the entity based on hive metadata
        qualified_name = f"{cluster_name}@{db_name}.{table_name}"
        # get guid of the entity that you want to delete
        guid = self.finder.get_guid_by_qualified_name("hive_table", qualified_name)
        self.hive_table_manager.delete_entity(guid)
        if purge:
            self.hive_table_manager.purge_entity(guid)

