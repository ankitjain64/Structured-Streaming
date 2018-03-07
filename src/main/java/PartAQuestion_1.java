import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import utils.Constants;

import static java.lang.Boolean.FALSE;
import static org.apache.spark.sql.streaming.OutputMode.Complete;
import static utils.IOUtils.*;

/**
 * Takes as argument the monitoring directory
 * <p>
 * emits the number of retweets (RT), mention (MT) and reply (RE) for an hourly window
 * that is updated every 30 minutes based on the timestamps of the tweets.
 * <p>
 * OutputMode as Console
 */
public class PartAQuestion_1 {
    private static final String SLIDE_DURATION = "30 minutes";
    private static final String WINDOW_DURATION = "60 minutes";

    public static void main(String[] args) throws InterruptedException {
        checkPreCondition(args);
        String monitoringDir = HDFS_PREFIX + args[0];
        String partAQuestion_1 = "PartAQuestion_1_" + Constants.getCurrentTime();
        SparkSession sparkSession = SparkSession.builder().appName(partAQuestion_1).getOrCreate();
        Dataset<Row> tweetDataset = getTweetDataset(monitoringDir, sparkSession);
        Column timeStampColumn = new Column(Constants.TIMESTAMP);
        Column tweetType = new Column(Constants.INTERACTION);
        Dataset<Row> tweetViewDataSet = tweetDataset.select(timeStampColumn, tweetType);
        Column windowColumn = functions.window(timeStampColumn, WINDOW_DURATION, SLIDE_DURATION);
        RelationalGroupedDataset relationalGroupedDataset = tweetViewDataSet.groupBy(windowColumn);
        StreamingQuery query = startAndPrint(relationalGroupedDataset);
        query.awaitTermination();
    }

    private static StreamingQuery startAndPrint(RelationalGroupedDataset relationalGroupedDataset) {
        return relationalGroupedDataset.count()
                .writeStream().format(CONSOLE)
                .outputMode(Complete())
                .option("truncate", FALSE.toString())
                .option("numRows", 1000000)
                .start();
    }
}
