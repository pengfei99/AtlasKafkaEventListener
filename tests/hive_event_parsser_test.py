import json
from event_parser.hive_event_parser import parse_hive_metastore_table_event_message


def test_parse_hive_metastore_table_event_message():
    test_msg = """{"tableName":"students","dbName":"default","owner":"pliu","createTime":1647683673,"lastAccessTime":0,
    "retention":0,"sd":{"cols":[{"name":"student_id","type":"int","comment":null,"setType":true,"setName":true,
    "setComment":false},{"name":"firstname","type":"string","comment":null,"setType":true,"setName":true,
    "setComment":false},{"name":"lastname","type":"string","comment":null,"setType":true,"setName":true,
    "setComment":false},{"name":"year","type":"string","comment":null,"setType":true,"setName":true,"setComment":false},
    {"name":"major","type":"string","comment":null,"setType":true,"setName":true,"setComment":false}],
    "location":"file:/home/pliu/hive_data/sample_data","inputFormat":"org.apache.hadoop.mapred.TextInputFormat",
    "outputFormat":"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat","compressed":false,"numBuckets":-1,
    "serdeInfo":{"name":null,"serializationLib":"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    "parameters":{"field.delim":",","serialization.format":","},"description":null,"serializerClass":null,
    "deserializerClass":null,"serdeType":null,"setParameters":true,"parametersSize":2,"setName":false,
    "setDescription":false,"setSerdeType":false,"setSerializationLib":true,"setSerializerClass":false,
    "setDeserializerClass":false},"bucketCols":[],"sortCols":[],"parameters":{},"skewedInfo":{"skewedColNames":[],
    "skewedColValues":[],"skewedColValueLocationMaps":{},"setSkewedColNames":true,"setSkewedColValues":true,
    "setSkewedColValueLocationMaps":true,"skewedColNamesSize":0,"skewedColNamesIterator":[],"skewedColValuesSize":0,
    "skewedColValuesIterator":[],"skewedColValueLocationMapsSize":0},"storedAsSubDirectories":false,"colsSize":5,
    "setParameters":true,"setLocation":true,"setInputFormat":true,"parametersSize":0,"setCols":true,
    "colsIterator":[{"name":"student_id","type":"int","comment":null,"setType":true,"setName":true,"setComment":false},
    {"name":"firstname","type":"string","comment":null,"setType":true,"setName":true,"setComment":false},
    {"name":"lastname","type":"string","comment":null,"setType":true,"setName":true,"setComment":false},
    {"name":"year","type":"string","comment":null,"setType":true,"setName":true,"setComment":false},
    {"name":"major","type":"string","comment":null,"setType":true,"setName":true,"setComment":false}],
    "setSkewedInfo":true,"setOutputFormat":true,"setCompressed":false,"setNumBuckets":true,"bucketColsSize":0,
    "bucketColsIterator":[],"sortColsSize":0,"sortColsIterator":[],"setStoredAsSubDirectories":true,"setSortCols":true,
    "setSerdeInfo":true,"setBucketCols":true},"partitionKeys":[],"parameters":{"totalSize":"62","EXTERNAL":"TRUE",
    "numFiles":"1","transient_lastDdlTime":"1647683673","bucketing_version":"2","comment":"Student Names"},
    "viewOriginalText":null,"viewExpandedText":null,"tableType":"EXTERNAL_TABLE","privileges":{"userPrivileges":{"pliu":
    [{"privilege":"INSERT","createTime":-1,"grantor":"pliu","grantorType":"USER","grantOption":true,"setPrivilege":true,
    "setGrantOption":true,"setCreateTime":true,"setGrantor":true,"setGrantorType":true},{"privilege":"SELECT",
    "createTime":-1,"grantor":"pliu","grantorType":"USER","grantOption":true,"setPrivilege":true,"setGrantOption":true,
    "setCreateTime":true,"setGrantor":true,"setGrantorType":true},{"privilege":"UPDATE","createTime":-1,"grantor":"pliu",
    "grantorType":"USER","grantOption":true,"setPrivilege":true,"setGrantOption":true,"setCreateTime":true,
    "setGrantor":true,"setGrantorType":true},{"privilege":"DELETE","createTime":-1,"grantor":"pliu","grantorType":"USER",
    "grantOption":true,"setPrivilege":true,"setGrantOption":true,"setCreateTime":true,"setGrantor":true,
    "setGrantorType":true}]},"groupPrivileges":null,"rolePrivileges":null,"setUserPrivileges":true,
    "setGroupPrivileges":false,"setRolePrivileges":false,"userPrivilegesSize":1,"groupPrivilegesSize":0,
    "rolePrivilegesSize":0},"temporary":false,"rewriteEnabled":false,"creationMetadata":null,"catName":"hive",
    "ownerType":"USER","partitionKeysSize":0,"setCatName":true,"setParameters":true,"setPartitionKeys":true,
    "setSd":true,"setPrivileges":true,"setDbName":true,"setTableName":true,"setCreateTime":true,
    "setLastAccessTime":false,"parametersSize":6,"setRetention":false,"partitionKeysIterator":[],"setTemporary":true,
    "setRewriteEnabled":false,"setOwner":true,"setViewOriginalText":false,"setViewExpandedText":false,
    "setTableType":true,"setCreationMetadata":false,"setOwnerType":true}"""
    tmp_table = json.loads(test_msg)
    expected_db_name = tmp_table["dbName"]
    expected_table_name = tmp_table['tableName']
    expected_owner = tmp_table['owner']
    expected_create_time = tmp_table["createTime"]
    expected_cols = tmp_table["sd"]["cols"]
    expected_location = tmp_table["sd"]["location"]

    db_name, table_name, owner, create_time, cols, location = parse_hive_metastore_table_event_message(test_msg)
    assert expected_db_name == db_name
    assert expected_table_name == table_name
    assert expected_owner == owner
    assert expected_create_time == create_time
    assert expected_cols == cols
    assert expected_location == location
