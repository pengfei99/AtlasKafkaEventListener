import json

from atlas_client.client import Atlas
from atlas_client.entity_management.hive.HiveColumnManager import HiveColumnManager
from atlas_client.entity_management.hive.HiveDBManager import HiveDBManager
from atlas_client.entity_management.hive.HiveTableManager import HiveTableManager
from atlas_client.entity_search.EntityFinder import EntityFinder

from atlas_kafka_event_listener.LogManager import LogManager
from atlas_kafka_event_listener.oidc.OidcTokenManager import OidcTokenManager

my_logger = LogManager(__name__).get_logger()
my_logger.debug("Init HiveEventHandler")


class HiveEventHandler:
    def __init__(self, atlas_hostname: str, atlas_port: int, oidc_token_manager: OidcTokenManager):
        self.__atlas_hostname = atlas_hostname
        self.__atlas_port = atlas_port
        self.__oidc_token_manager = oidc_token_manager
        self.__atlas_client = Atlas(host=self.__atlas_hostname, port=self.__atlas_port,
                                    oidc_token=self.__oidc_token_manager.get_current_access_token())
        self.__hive_db_manager = HiveDBManager(self.__atlas_client)
        self.__hive_table_manager = HiveTableManager(self.__atlas_client)
        self.__hive_column_manager = HiveColumnManager(self.__atlas_client)
        self.__finder = EntityFinder(self.__atlas_client)

    def handle_create_table_event(self, event_msg: str):
        """
        This method handles the hive create table event. It will create the db, table and columns metadata in Atlas
        based on the given event message
        :param event_msg: The event message send by hive metastore listener
        :return: Return true if all creation is success. Otherwise, it returns false
        """
        if self.__oidc_token_manager.renew_oidc_token():
            self.__renew_meta_managers()
        table_event_metadata = json.loads(event_msg)
        table_name = table_event_metadata.get('tableName').lower()
        db_name = table_event_metadata.get('dbName').lower()
        owner = table_event_metadata.get('owner')
        create_time = table_event_metadata.get("createTime")
        cols = table_event_metadata.get("sd").get("cols")
        data_location = table_event_metadata.get("sd").get("location")

        table_description = table_event_metadata.get("parameters").get("comment")
        try:
            # step 1 create/update hive db
            # here cluster name is the name space of each user(e.g. KUBERNETES_NAMESPACE=user-pengfei)
            cluster_name = table_event_metadata.get("clusterName")
            db_description = ""
            self.__hive_db_manager.create_entity(db_name, cluster_name, db_description, owner=owner)
        except Exception as e:
            my_logger.exception(f"Can't create hive database with name: {db_name}")
            return False
        try:
            # step 2 create/update table
            db_qualified_name = f"{cluster_name}@{db_name}"
            self.__hive_table_manager.create_entity(table_name, db_qualified_name, table_description, owner=owner,
                                                    create_time=create_time)
        except Exception as e:
            my_logger.exception(f"Can't create hive table with name: {table_name} in database {db_qualified_name}")
            return False
        try:
            # step 3 creat columns
            table_qualified_name = f"{db_qualified_name}.{table_name}"
            for col in cols:
                col_name = col.get("name").lower()
                col_type = col.get("type")
                col_description = col.get("comment")
                self.__hive_column_manager.create_entity(col_name, col_type, table_qualified_name,
                                                         col_description)
        except Exception as e:
            my_logger.exception(f"Can't create hive column with name: {db_name} in hive table {table_qualified_name}")
            return False
        return True

    def handle_drop_table_event(self, event_msg: str, purge=True):
        """
        This method handles the hive drop table event. It will remove the metadata of the hive table based on the given
        event message from the atlas server.
        :param event_msg: The event message send by hive metastore listener
        :param purge: If purge set to True, it will purge(delete permanently) the deleted metadata
        :return: Return true if the deletion is success. Otherwise, it returns false
        """
        if self.__oidc_token_manager.renew_oidc_token():
            self.__renew_meta_managers()
        table_event_metadata = json.loads(event_msg)
        table_name = table_event_metadata.get('tableName').lower()
        db_name = table_event_metadata.get('dbName').lower()
        cluster_name = table_event_metadata.get("clusterName")
        # build qualified name of the entity based on hive metadata
        qualified_name = f"{cluster_name}@{db_name}.{table_name}"
        # get guid of the entity that you want to delete
        try:
            guid = self.__finder.get_guid_by_qualified_name("hive_table", qualified_name)
        except Exception as e:
            my_logger.exception(f"Can't find guid for given entity qualified_name {qualified_name}")
            return False
        else:
            self.__hive_table_manager.delete_entity(guid)
            if purge:
                self.__hive_table_manager.purge_entity(guid)
        return True

    def __renew_meta_managers(self):
        """
        This method renew the atlas client and all managers that use the client with the new
         renewed oidc token, when the oidc token is renewed after expiration
        :return:
        """
        self.__atlas_client = Atlas(host=self.__atlas_hostname, port=self.__atlas_port,
                                    oidc_token=self.__oidc_token_manager.get_current_access_token())
        self.__hive_db_manager = HiveDBManager(self.__atlas_client)
        self.__hive_table_manager = HiveTableManager(self.__atlas_client)
        self.__hive_column_manager = HiveColumnManager(self.__atlas_client)
        self.__finder = EntityFinder(self.__atlas_client)
