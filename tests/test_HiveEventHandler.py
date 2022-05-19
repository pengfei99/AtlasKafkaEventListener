import pytest

from atlas_kafka_event_listener.HiveEventHandler import HiveEventHandler
from atlas_kafka_event_listener.oidc.OidcTokenManager import OidcTokenManager
from creds import client_secret


@pytest.fixture(scope='session')
def event_handler():
    local = False
    # config for atlas client
    if local:
        atlas_hostname = "http://localhost"
        atlas_port = 80
    else:
        atlas_hostname = "https://atlas.lab.sspcloud.fr"
        atlas_port = 443
    # config for oidc token manager
    keycloak_server_url = "https://auth.lab.sspcloud.fr/auth"
    realm_name = "sspcloud"
    client_id = "atlas-sa"
    oidc_token_manager = OidcTokenManager(keycloak_server_url, realm_name, client_id, client_secret)
    event_handler = HiveEventHandler(atlas_hostname, atlas_port, oidc_token_manager)
    return event_handler


def test_parse_create_table_event_message(event_handler):
    test_msg = """{"tableName":"toto","dbName":"default","owner":"pliu","createTime":1649170513,"lastAccessTime":0,
                  "retention":0,"sd":{"cols":[{"name":"student_id","type":"int"},{"name":"firstname","type":"string"},
                  {"name":"lastname","type":"string"},{"name":"year","type":"string"},{"name":"major","type":"string"}],
                  "location":"s3a://pengfei/diffusion/hive_data/sample_data","inputFormat":"org.apache.hadoop.mapred.TextInputFormat",
                  "outputFormat":"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat","compressed":false,"numBuckets":-1,
                  "serdeInfo":{"serializationLib":"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                  "parameters":{"field.delim":",","serialization.format":","}},"bucketCols":[],"sortCols":[],"parameters":{},
                  "skewedInfo":{"skewedColNames":[],"skewedColValues":[],"skewedColValueLocationMaps":{}},
                  "storedAsSubDirectories":false,"__isset_bitfield":6},"partitionKeys":[],"parameters":{"totalSize":"62","EXTERNAL":"TRUE",
                  "numFiles":"1","transient_lastDdlTime":"1649170513","bucketing_version":"2","comment":"Student Names"},
                  "tableType":"EXTERNAL_TABLE","privileges":{"userPrivileges":{"pliu":[{"privilege":"INSERT","createTime":-1,
                  "grantor":"pliu","grantorType":"USER","grantOption":true,"__isset_bitfield":3},{"privilege":"SELECT",
                  "createTime":-1,"grantor":"pliu","grantorType":"USER","grantOption":true,"__isset_bitfield":3},
                  {"privilege":"UPDATE","createTime":-1,"grantor":"pliu","grantorType":"USER","grantOption":true,"__isset_bitfield":3},
                  {"privilege":"DELETE","createTime":-1,"grantor":"pliu","grantorType":"USER","grantOption":true,"__isset_bitfield":3}]}},
                  "temporary":false,"rewriteEnabled":false,"catName":"hive","ownerType":"USER","__isset_bitfield":9,"clusterName":"user-pengfei"}"""

    event_handler.handle_create_table_event(test_msg)


def test_parse_drop_table_event_message(event_handler):
    test_msg = """{"tableName":"TesT","dbName":"default","owner":"pliu","createTime":1649169136,"lastAccessTime":0,
    "retention":0,"sd":{"cols":[{"name":"student_id","type":"int"},{"name":"firstname","type":"string"},
    {"name":"lastname","type":"string"},{"name":"year","type":"string"},{"name":"major","type":"string"}],
    "location":"file:/home/pliu/hive_data/sample_data","inputFormat":"org.apache.hadoop.mapred.TextInputFormat",
    "outputFormat":"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat","compressed":false,"numBuckets":-1,
    "serdeInfo":{"serializationLib":"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    "parameters":{"serialization.format":",","field.delim":","}},"bucketCols":[],"sortCols":[],"parameters":{},
    "skewedInfo":{"skewedColNames":[],"skewedColValues":[],"skewedColValueLocationMaps":{}},
    "storedAsSubDirectories":false,"__isset_bitfield":7},"partitionKeys":[],"parameters":{"totalSize":"62",
    "EXTERNAL":"TRUE","numFiles":"1","transient_lastDdlTime":"1649169136","bucketing_version":"2",
    "comment":"Student Names"},"tableType":"EXTERNAL_TABLE","temporary":false,"rewriteEnabled":false,
    "catName":"hive","ownerType":"USER","__isset_bitfield":23,"clusterName":"user-pengfei"}"""

    event_handler.handle_drop_table_event(test_msg, purge=True)
