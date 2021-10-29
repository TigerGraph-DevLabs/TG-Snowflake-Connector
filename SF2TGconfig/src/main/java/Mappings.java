import java.util.List;

public class Mappings {
    public String SFTable;
    public String tgLoadJob;
    public List<String> sfColumns;
    public String fileName;

    public List<String> getsfColumns() {
        return sfColumns;
    }

    public void setsfColumns(List<String> sfColumns) {
        this.sfColumns = sfColumns;
    }

    public String getfileName() {
        return fileName;
    }

    public void setfileName(String fileName) {
        this.fileName = fileName;
    }

    public String getSFTable() {
        return SFTable;
    }

    public void setSFTable(String SFTable) {
        this.SFTable = SFTable;
    }

    public String gettgLoadJob() {
        return tgLoadJob;
    }

    public void settgLoadJob(String tgLoadJob) {
        this.tgLoadJob = tgLoadJob;
    }

}
