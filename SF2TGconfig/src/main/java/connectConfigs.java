import java.util.ArrayList;

public class connectConfigs {
    // Snowflake Info
    public String sfURL;
    public String sfUser;
    public String sfPassword;
    public String sfDatabase;
    public String sfSchema;

    public ArrayList<String> sfDbTable = new ArrayList<>();

    // TG info
    public String tgIP;
    public String driver;
    public String url;
    public String username;
    public String password;
    public String token;
    public String graph;

    public int batchSize = 5000;
    public String sep = ",";
    public String eol = "\\n";
    public int debug = 0;
    public int numPartitions = 150;

    public ArrayList<Mappings> mappingRules = new ArrayList<>();

    public String getTgIP() {
        return tgIP;
    }

    public void setTgIP(String tgIP) {
        this.tgIP = tgIP;
    }

    public String getSfURL() {
        return sfURL;
    }

    public void setSfURL(String sfUrl) {
        this.sfURL = sfUrl;
    }

    public String getSfUser() {
        return sfUser;
    }

    public void setSfUser(String sfUser) {
        this.sfUser = sfUser;
    }

    public String getSfPassword() {
        return sfPassword;
    }

    public void setSfPassword(String sfPassword) {
        this.sfPassword = sfPassword;
    }

    public String getSfDatabase() {
        return sfDatabase;
    }

    public void setSfDatabase(String sfDatabase) {
        this.sfDatabase = sfDatabase;
    }

    public String getSfSchema() {
        return sfSchema;
    }

    public void setSfSchema(String sfSchema) {
        this.sfSchema = sfSchema;
    }

    public ArrayList<String> getSfDbTable() {
        return sfDbTable;
    }

    public void setSfDbTable(ArrayList<String> sfDbTable) {
        this.sfDbTable = sfDbTable;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl() {
        // jdbc:tg:http://<MACHINE_IP>:14240
        this.url = "jdbc:tg:http://" + this.tgIP + ":14240";
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph = graph;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getSep() {
        return sep;
    }

    public void setSep(String sep) {
        this.sep = sep;
    }

    public String getEol() {
        return eol;
    }

    public void setEol(String eol) {
        this.eol = eol;
    }

    public int getDebug() {
        return debug;
    }

    public void setDebug(int debug) {
        this.debug = debug;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public ArrayList<Mappings> getmappingRules() {
        return mappingRules;
    }

    public void setmappingRules(Mappings mappingInfo) {
        this.mappingRules.add(mappingInfo);
    }

}