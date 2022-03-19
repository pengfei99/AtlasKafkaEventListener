import json


def parse_hive_metastore_table_event_message(event_msg: str):
    table_event_metadata = json.loads(event_msg)
    table_name = table_event_metadata['tableName']
    db_name = table_event_metadata['dbName']
    owner = table_event_metadata['owner']
    create_time = table_event_metadata["createTime"]
    cols = table_event_metadata["sd"]["cols"]
    data_location = table_event_metadata["sd"]["location"]

    return db_name, table_name, owner, create_time, cols, data_location

#   hive_db.create_entity("pengfei-stock", "pengfei.org", "database for my stock market")
#     hive_table.create_entity("favorite", "pengfei.org@pengfei-stock", "favorite stock")
#     hive_column.create_entity("stock_id", "int", "pengfei.org@pengfei-stock.favorite", "id of the stock")
#     hive_column.create_entity("stock_name", "string", "pengfei.org@pengfei-stock.favorite", "name of the stock")
#
