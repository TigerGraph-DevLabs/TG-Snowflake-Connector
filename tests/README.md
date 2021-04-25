

## 1. Prerequisite

**Before you begin, make sure you have the following environment**

- Spark 2.4.6+ environment available, standalone or other mode set-up
- Snowflake environment
- TigerGraph 3.1.1+

If you don't have Spark installed, follow these steps to install it

[Spark homepage](spark.apache.org)

## 2. Preparation 

With the exception of the project jar, you can find the other file in the friendNet directory.

1. Execute the SQL file in snowflake(init.sql).

2. Execute GSQL under the TigerGraph user.

```
$ gsql graph_create.gsql
```

3. Change the snowflake and TigerGraph config in the configuration file.

Note: modify friend_net_connector.yaml in the friendNet folder

```yaml
# snowflake config
sfURL: lla10179.us-east-1.snowflakecomputing.com
sfUser: liusx
sfPassword: WxW7b6xJtWJpUGt

# tigergraph config
driver: com.tigergraph.jdbc.Driver
url: jdbc:tg:http://192.168.100.21:14240
username: tigergraph
password: ces123
# This is not required if authentication is not turned on
token: k670rl16oncs359la8tf2upb5jlmrfg0
```

## 3. Execute the task

You can find the task jar package in the assets directory under the project root.

[TigerGraphConnector.jar](../assets/TigerGraphConnector.jar)

```shell
$ pwd
/opt/spark_connector/tests

$ ls
friendNet/  README.md

$ $SPARK_HOME/bin/spark-submit --class com.tigergraph.spark_connector.SF2Spark2Tg --master spark://$SPARK_IP:7077 /opt/spark_connector/assets/TigerGraphConnector.jar /opt/spark_connector/tests/frientNet/friend_net_connector.yaml
```

Finally, if the following prompt appears, the import is successful

```
21/04/25 11:36:33 INFO progress: The total time consuming:15s
21/04/25 11:36:33 INFO progress: Write Data Progress: ************************************************** |100%
```

