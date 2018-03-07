import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import utils.Constants;
import utils.IOUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static utils.Constants.INTERACTION;
import static utils.Constants.USER_A;
import static utils.IOUtils.CONSOLE;
import static utils.IOUtils.HDFS_PREFIX;

/**
 * Takes as argument the monitoring directory
 * <p>
 * takes as input the list of twitter user IDs and every 5 seconds,
 * it emits the number of tweet actions of a user if it is present in the input list.
 */
public class PartAQuestion_3 {
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("First Argument as monitoring " +
                    "dir and rest as the twitter user id's");
        }
        String monitoringDir = HDFS_PREFIX + args[0];
        List<String> userList = new ArrayList<String>();//convert to set
        userList.addAll(Arrays.asList(args).subList(1, args.length));
        userList = new ArrayList<String>(new HashSet<String>(userList));
        String appName = "PartAQuestion_3_" + Constants.getCurrentTime();
        SparkSession sparkSession = SparkSession.builder().appName(appName).getOrCreate();
        Dataset<Row> tweetDataset = IOUtils.getTweetDataset(monitoringDir, sparkSession);
        Dataset<Row> userDataSet = IOUtils.getStaticUserData(sparkSession, userList);
        Column userAColumn = new Column(USER_A);
        Column interactionColumn = new Column(INTERACTION);
        Dataset<Row> joinedDataSet = tweetDataset.join(userDataSet, USER_A);
        RelationalGroupedDataset relationalGroupedDataset = joinedDataSet.select(userAColumn, interactionColumn).groupBy(userAColumn);
        ProcessingTime trigger = ProcessingTime.create(5L, SECONDS);
        StreamingQuery query = startAndPrintStream(relationalGroupedDataset, trigger);
        query.awaitTermination();
    }

    private static StreamingQuery startAndPrintStream(RelationalGroupedDataset relationalGroupedDataset, ProcessingTime trigger) {
        return relationalGroupedDataset.count().writeStream()
                .outputMode(OutputMode.Complete())
                .format(CONSOLE)
                .option("truncate", Boolean.FALSE.toString())
                .option("numRows", 1000000)
                .trigger(trigger)
                .start();
    }
}
