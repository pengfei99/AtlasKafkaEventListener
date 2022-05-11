# AtlasKafkaEventListener

## Installation

### Install it from source

#### Step 1. Clone the project source

```shell
git clone https://github.com/pengfei99/AtlasKafkaEventListener.git

```

#### Step 2. Install the project dependencies
```shell 
cd AtlasKafkaEventListener
pip install -r requirements.txt 
```

#### Step 3. Setup env var,

Note if log level is not defined, it will use DEBUG as default log level

```shell
export ATLAS_HOSTNAME=https://atlas.lab.sspcloud.fr
export ATLAS_PORT=443
export OIDC_TOKEN=
export KAFKA_BROKER_URL=hadoop1.insee.fr:9092,hadoop2.insee.fr:9092,hadoop3.insee.fr:9092
export KAFKA_TOPIC_NAME=hive-meta
export CONSUMER_GROUP_ID=hive_atlas_meta
export LOGLEVEL=INFO
export PYTHONPATH="${PYTHONPATH}:/path/to/AtlasKafkaEventListener"
```


##### You must configure python path
Without the python path configuration, the app will not work. 

```shell
# You need to include path of code source in the python path 
export PYTHONPATH="${PYTHONPATH}:/path/to/AtlasKafkaEventListener"

# example
export PYTHONPATH="${PYTHONPATH}:/home/jovyan/work/AtlasKafkaEventListener"
```


There is a config file with prefill values, you can just edit it with appropriate values and source it

```shell
vim AtlasKafkaEventListener/command/set_env.sh
source AtlasKafkaEventListener/command/set_env.sh
```
#### Step 4. Run the app

There is a lanch script under command folder

```shell
cd AtlasKafkaEventListener/command
sh run.sh
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
