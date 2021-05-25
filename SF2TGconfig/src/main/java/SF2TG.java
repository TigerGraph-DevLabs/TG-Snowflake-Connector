import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class SF2TG {
    public static Connection connection;
    // set of table names
    public static ArrayList<String> tables;
    // table name -> column names
    public static HashMap<String, List<String>> tableMap;
    // loading job names -> filenames
    public static HashMap<String, Set<String>> tgMap;
    // snowflake -> tigergraph loading jobs
    public static HashMap<String, String> jobMap = new HashMap<>();

    public static void main (String[] args) throws SQLException, InterruptedException, IOException {
        connectConfigs config = new connectConfigs();
        ArrayList<String> jobs = new ArrayList<>();

        if (args.length == 0 || (args.length == 1 && !args[0].equals("-h"))) {
            showHelp();
            System.exit(0);
        } else if (args[0].equals("-h")) {
            showHelp();
        } else {
            for (String arg : args) {
                String[] splitArg = arg.split("=", 2);
                switch (splitArg[0]) {
                    case "sfuser":
                        config.setSfUser(splitArg[1]);
                        break;
                    case "sfpassword":
                        config.setSfPassword(splitArg[1]);
                        break;
                    case "sfurl":
                        config.setSfURL(splitArg[1]);
                        break;
                    case "sfdb":
                        config.setSfDatabase(splitArg[1]);
                        break;
                    case "sfschema":
                        config.setSfSchema(splitArg[1]);
                        break;
                    case "tguser":
                        config.setUsername(splitArg[1]);
                        break;
                    case "tgpassword":
                        config.setPassword(splitArg[1]);
                        break;
                    case "tgip":
                        config.setTgIP(splitArg[1]);
                        config.setDriver("com.tigergraph.jdbc.Driver");
                        config.setUrl();
                        break;
                    case "tgtoken":
                        config.setToken(splitArg[1]);
                        break;
                    case "graph":
                        config.setGraph(splitArg[1]);
                        break;
                    case "batchSize":
                        config.setBatchSize(Integer.parseInt(splitArg[1]));
                        break;
                    case "sep":
                        config.setSep(splitArg[1]);
                        break;
                    case "eol":
                        config.setEol(splitArg[1]);
                        break;
                    case "debug":
                        config.setDebug(Integer.parseInt(splitArg[1]));
                        break;
                    case "numPartitions":
                        config.setNumPartitions(Integer.parseInt(splitArg[1]));
                        break;
                    default:
                        jobs.add(splitArg[0]);
                        break;
                }
            }

            try {
                for (String s : jobs) {
                    String[] sepJobs = s.split(":",2);
                    jobMap.put(sepJobs[0].toUpperCase(),sepJobs[1]);
                }
            } catch (NullPointerException e) {
                showHelp();
                System.exit(0);
            }


            // check if essential config information is missing
            if (!checkEssentials(config)) {
                System.err.println("Missing essential information. Check inputs");
                System.exit(0);
            }

            // connect to SF server
            System.out.println("Connecting to Snowflake");
            connection = SFConnection.getConnection(config);
            if (connection == null) {
                System.err.println("Unable to connect to Snowflake. Please check your provided credentials");
                System.exit(0);
            }
            System.out.println("Snowflake Connected\n");

            // get tables from SF DB
            tables = SFConnection.getTables(config);
            // get columns per table
            tableMap = SFConnection.getColumns(tables, config);

            // close SF connection
            connection.close();

            if (tables.size() == 0 || tableMap.size() == 0) {
                System.err.println("Invalid database or schema, please verify your Snowflake DB.");
                System.exit(0);
            }

            for (String s : jobMap.keySet()) {
                if (!tables.contains(s.toUpperCase())) {
                    System.err.println("Table Name : " + s + " does not exist in Snowflake");
                    System.exit(0);
                }
            }

            System.out.println("\nSnowflake tables: " + tables);
            System.out.println("Snowflake columns: " + tableMap + "\n");

            //////////////////////////////////////////////////////////////////////////////////
            // Generate TG GraphStudio session cookie and get loading job name -> filenames //
            //////////////////////////////////////////////////////////////////////////////////
            // test TG token validity
            if (config.getToken() != null) {
                if (!TGConnection.testToken(config)) {
                    System.err.println("Invalid Token for TG Graph: " + config.getGraph() + ". Exiting...");
                    System.exit(0);
                }
            }

            // check TigerGraph user credentials
            if (!TGConnection.checkUser(config)) {
                System.err.println("TigerGraph user login invalid.");
                System.exit(0);
            }

            // check graph access
            if (!TGConnection.graphAccess(config)) {
                System.err.println("This TigerGraph user either does not have access to this graph " +
                        "or this graph does not exist.");
                System.exit(0);
            }

            // check tg credentials validity, if false, exit with error.
            if (!TGConnection.testLogin(config)) {
                System.err.println("Cannot access graph. Please check your TigerGraph instance.");
                System.exit(0);
            }

            tgMap = TGConnection.getTGInfo(config);
            if (tgMap == null || tgMap.size() == 0) {
                System.err.println("Unable to retrieve TigerGraph loading jobs. Please check your TigerGraph Instance.");
                System.exit(0);
            } else {
                System.out.println("TG loading jobs: " + tgMap + "\n");
            }

            // check loading job existence input : tigergraph instance
            for (String s : jobMap.values()) {
                if (!tgMap.containsKey(s)) {
                    System.err.println("Loading job: " + s + " does not exist in your TigerGraph instance");
                    System.exit(0);
                }
            }

            /////////////////////////////////////////////////
            // Dump the collected information to yaml file //
            /////////////////////////////////////////////////
            System.out.println("Printing to YAML file: " + System.getProperty("user.dir") + "/connector.yaml");
            YamlCreation.writeToYaml(tables,jobMap, tableMap, tgMap, config);
        }
    }

    public static boolean checkEssentials(connectConfigs config) {
        return config.getSfURL() != null && config.getSfUser() != null && config.getSfPassword() != null &&
                config.getSfDatabase() != null && config.getSfSchema() != null && config.getTgIP() != null &&
                !(config.getUsername() == null | config.getPassword() == null) && config.getGraph() != null;
    }

    public static void showHelp() {
        System.out.println("Usage: ./SF2TG [-h] [sfuser=<SFUsername> sfpassword=<SFPassword> sfurl=<SFURL> " +
                "sfdb=<SFDatabase> sfschema=<SFSchema> tguser=<TGUsername> tgpassword=<TGPassword> " +
                "tgip=<TGMachineIP> tgtoken=<TGToken> graph=<TGGraphName>] [eol=<end_of_line_symbol] " +
                "[sep=<column_delimiter>] [numPartitions=<loading_partitions>] [batchSize=<loading_batchSize>] " +
                "[debug=<spark_debug_value>]" +
                " [<SFTablename1:TGLoadingJob1 SFTablename2:TGLoadingJob2 ...>]");
    }
}
