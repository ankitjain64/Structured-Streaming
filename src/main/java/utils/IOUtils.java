package utils;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import static utils.Constants.*;

public class IOUtils {
    private static final String COMMA = ",";
    public static final String CONSOLE = "console";
    public static final String HDFS_PARQUET = "parquet";
    public static final String HDFS_PREFIX = "hdfs://10.254.0.14:8020";

    public static void checkPreCondition(String[] args) {
        checkPreCondition(args, 1);
    }

    public static void checkPreCondition(String[] args, int count) {
        if (args.length != count) {
            throw new IllegalArgumentException("Provide monitoring dir as an " +
                    "argument");
        }
    }

    public static Dataset<Row> getTweetDataset(String monitoringDir,
                                               SparkSession sparkSession) {
        StructType schema = new StructType().add(USER_A, "string")
                .add(USER_B, "string")
                .add(TIMESTAMP, "string")
                .add(INTERACTION, "string");
        return sparkSession.readStream().format("csv").option
                ("sep", ",").schema(schema).load(monitoringDir);
    }

    public static Dataset<Row> getStaticUserData(SparkSession sparkSession,
                                                 List<String> userList) {
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
        JavaRDD<Row> map = javaSparkContext.parallelize(userList).map(new Function<String, Row>() {
            public Row call(String s) throws Exception {
                return RowFactory.create(s);
            }
        });
        StructType schema = new StructType().add(USER_A, "string");
        return sparkSession.createDataFrame(map, schema);
    }
}
