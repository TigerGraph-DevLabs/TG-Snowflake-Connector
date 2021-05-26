# TigerGraph-Snowflake-Connector



## 1. Introduction

This project enables users to load data from Snowflake database into TigerGraph database easily. For more details, refer to the documentation page:  https://github.com/TigerGraph-DevLabs/TG-Snowflake-Connector/wiki/Snowflake-Connector-Documentation

If you are a developer and would like to look at the source code, the code is located in 'src' folder.

If you just want to use the connector, you can find the artifacts in 'assets' folder or review 'tests' folder.

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

