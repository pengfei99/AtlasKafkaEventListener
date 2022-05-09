# AtlasKafkaEventListener

## Installation

### Install it from source

Setup env var,
```shell
export ATLAS_HOSTNAME=https://atlas.lab.sspcloud.fr
export ATLAS_PORT=443
export OIDC_TOKEN=
export KAFKA_BROKER_URL=hadoop1.insee.fr:9092,hadoop2.insee.fr:9092,hadoop3.insee.fr:9092
export KAFKA_TOPIC_NAME=hive-meta
export CONSUMER_GROUP_ID=hive_atlas_meta
export PYTHONPATH="${PYTHONPATH}:/path/to/AtlasKafkaEventListener"
```
# configure python path

```shell
# You need to include path of code source in the python path 
export PYTHONPATH="${PYTHONPATH}:/path/to/AtlasKafkaEventListener"

# example
export PYTHONPATH="${PYTHONPATH}:/home/jovyan/work/AtlasKafkaEventListener"
```

### Install via k8s/docker



# hive commands

### create table
```sql
CREATE TABLE orders (
  order_id INT COMMENT 'Unique order id',
  order_date STRING COMMENT 'Date on which order is placed',
  order_customer_id INT COMMENT 'Customer id who placed the order',
  order_status STRING COMMENT 'Current status of the order'
) COMMENT 'Table to save order level details';
```

### alter table
Add column to a table
```sql
ALTER TABLE MyTest ADD COLUMNS (age INT COMMENT 'Student age');
```
