import java.util.*;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.snowflake.client.core.QueryStatus;
import net.snowflake.client.jdbc.SnowflakeResultSet;
import net.snowflake.client.jdbc.SnowflakeStatement;

public class SFConnection {

    public static ArrayList<String> getTables(connectConfigs config) throws SQLException, InterruptedException {
        ArrayList<String> res = new ArrayList<>();

        String sql_command;
        ResultSet resultSet;

        System.out.println("Getting tables");
        Statement statement = SF2TG.connection.createStatement();
        sql_command = "select table_name from information_schema.tables where table_schema ilike '" + config.getSfSchema() + "' order by table_name";

        resultSet = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(sql_command);

        // Assume that the query isn't done yet.
        QueryStatus queryStatus = QueryStatus.RUNNING;
        while (queryStatus == QueryStatus.RUNNING || queryStatus == QueryStatus.RESUMING_WAREHOUSE) {
            Thread.sleep(2000); // 2000 milliseconds.
            queryStatus = resultSet.unwrap(SnowflakeResultSet.class).getStatus();
        }

        if (queryStatus == QueryStatus.FAILED_WITH_ERROR) {
            // Print the error code to stdout
            System.out.format("Error code: %d%n", queryStatus.getErrorCode());
            System.out.format("Error message: %s%n", queryStatus.getErrorMessage());
        } else if (queryStatus != QueryStatus.SUCCESS) {
            System.out.println("ERROR: unexpected QueryStatus: " + queryStatus);
        } else {
            boolean result_exists = resultSet.next();
            if (!result_exists) {
                System.out.println("ERROR: No rows returned.");
            } else {
                res.add(resultSet.getString(1));
                while (resultSet.next()) {
                    res.add(resultSet.getString(1));
                }
            }
        }
        return res;
    }

    public static HashMap<String, List<String>> getColumns (ArrayList<String> tableSet, connectConfigs config) throws InterruptedException, SQLException {
        HashMap<String, List<String>> mapRes = new HashMap<>();

        for (String cur : tableSet) {
            String sql_command;
            ResultSet resultSet;

            System.out.println("Getting columns for table: " + cur);
            Statement statement = SF2TG.connection.createStatement();
            sql_command = "select column_name from information_schema.columns where table_schema ilike '" + config.getSfSchema() + "' and table_name ilike '" + cur + "' order by ordinal_position;";

            resultSet = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(sql_command);

            // Assume that the query isn't done yet.
            QueryStatus queryStatus = QueryStatus.RUNNING;
            while (queryStatus == QueryStatus.RUNNING || queryStatus == QueryStatus.RESUMING_WAREHOUSE) {
                Thread.sleep(2000); // 2000 milliseconds.
                queryStatus = resultSet.unwrap(SnowflakeResultSet.class).getStatus();
            }

            if (queryStatus == QueryStatus.FAILED_WITH_ERROR) {
                // Print the error code to stdout
                System.out.format("Error code: %d%n", queryStatus.getErrorCode());
                System.out.format("Error message: %s%n", queryStatus.getErrorMessage());
            } else if (queryStatus != QueryStatus.SUCCESS) {
                System.out.println("ERROR: unexpected QueryStatus: " + queryStatus);
            } else {
                boolean result_exists = resultSet.next();
                if (!result_exists) {
                    System.out.println("ERROR: No rows returned.");
                } else {
                    if (!mapRes.containsKey(cur)) {
                        mapRes.put(cur, new LinkedList<>());
                    }
                    mapRes.get(cur).add(resultSet.getString(1));
                    while (resultSet.next()) {
                        mapRes.get(cur).add(resultSet.getString(1));
                    }
                }
            }
        }
        return mapRes;
    }

    public static Connection getConnection(connectConfigs sfconfig) {
        try {
            Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
        } catch (ClassNotFoundException ex) {
            System.err.println("Driver not found");
        }

        try {
            // build connection properties
            Properties properties = new Properties();
            properties.put("user", sfconfig.getSfUser());     // replace "" with your username
            properties.put("password", sfconfig.getSfPassword()); // replace "" with your password
            properties.put("account", sfconfig.getSfURL());  // replace "" with your account name
            properties.put("db", sfconfig.getSfDatabase());       // replace "" with target database name
            properties.put("schema", sfconfig.getSfSchema());   // replace "" with target schema name
            //properties.put("tracing", "on");

            // create a new connection
            String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");

            // use the default connection string if it is not set in environment
            if (connectStr == null) {
                connectStr = "jdbc:snowflake://" + sfconfig.getSfURL() + ".snowflakecomputing.com"; // replace accountName with your account name
            }
            return DriverManager.getConnection(connectStr, properties);
        } catch (SQLException e) {
            System.err.println("Incorrect username or password");
        }
        return null;
    }
}
