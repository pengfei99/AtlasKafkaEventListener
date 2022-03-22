# AtlasKafkaEventListener

# configure python path

```shell
export PYTHONPATH="${PYTHONPATH}:/home/jovyan/work/AtlasKafkaEventListener"
```

# hive commands

```sql
CREATE TABLE orders (
  order_id INT COMMENT 'Unique order id',
  order_date STRING COMMENT 'Date on which order is placed',
  order_customer_id INT COMMENT 'Customer id who placed the order',
  order_status STRING COMMENT 'Current status of the order'
) COMMENT 'Table to save order level details';
```