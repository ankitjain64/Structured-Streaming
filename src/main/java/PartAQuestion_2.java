import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import utils.Constants;
import utils.IOUtils;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.spark.sql.streaming.OutputMode.Append;
import static utils.Constants.INTERACTION;
import static utils.IOUtils.*;

/**
 * Takes as argument the monitoring directory
 * <p>
 * emits the twitter IDs of users that have been mentioned by other users every 10 seconds.
 * Output is written to hdfs
 */
public class PartAQuestion_2 {
    private static final String MENTION_TYPE = "\'MT\'";

    public static void main(String[] args) {
        checkPreCondition(args, 2);
        String monitoringDir = HDFS_PREFIX + args[0];
        String outputPath = HDFS_PREFIX + args[1];
        String appName = "PartAQuestion_2" + Constants.getCurrentTime();
        SparkSession sparkSession = SparkSession.builder().appName(appName).getOrCreate();
        Dataset<Row> tweetDataset = IOUtils.getTweetDataset(monitoringDir, sparkSession);
        Column userMentioned = new Column(Constants.USER_B);
        Dataset<Row> userMentionedDataSet = tweetDataset.select(userMentioned)
                .filter(INTERACTION + " = " + MENTION_TYPE);
        ProcessingTime trigger = ProcessingTime.create(10L, SECONDS);
        StreamingQuery query = userMentionedDataSet.writeStream()
                .format(HDFS_PARQUET)
                .option("path", outputPath)
                .option("checkpointLocation", HDFS_PREFIX +
                        "/hw2/Part-A/Q2_checkpoint_" + Constants.getCurrentTime())
                .trigger(trigger)
                .outputMode(Append())
                .start();
        query.awaitTermination();
    }
}
