package cs455.project.utils;

public class Constants {
    public static final String HDFS_SERVER = "phoenix:54300";
    public static final String HDFS_CRIMES_DIR = String.format("hdfs://%s/cs455/project/crimes", HDFS_SERVER);
    public static final String HDFS_MOONS_DIR = String.format("hdfs://%s/cs455/project/moons", HDFS_SERVER);
    public static final String COMMA_STR = ",";
}
