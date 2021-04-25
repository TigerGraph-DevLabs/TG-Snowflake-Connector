# spark-connector



## 1. Introduction

The project is to write the data from snowFlake into the TigerGraph. The goal is to import data by modifying configuration file.

If you're a developer, the code is in src.

If you just want to use the connector, you can find it in assets or tests.

You can use QuickStart to get started with the connector.

## 2. QuickStart

[friendNet](./tests/README.md)

Detailed documentation location：[TigerGraphConnector_readme](./assets/TigerGraphConnector_readme.md)



## 3. Folder description

```shell
# Connector that can be used, Configuration file and connector usage documentation
assets/

# source code
src/

# Use cases
# including Snowflake table building statement
# TigerGraph construction sentences and Loading Job
tests/
```

## 4. Development environment

- Java 8+
- Maven 3.5.4+



## 5. Code structure

```
src.main
	|--resources
		|--connector.yaml(configuration file)
	|--scala
		|--com.tigergraph.spark_connector
			|--reader(data origin)
			|--support(Read the configured support class)
			|--utils
			|--writer
			|--SF2Spark2Tg(main class)
```

