import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SeoReducer extends Reducer<HostUrlPair, Text, Text, IntWritable> {


    @Override
    protected void reduce(HostUrlPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int queryCouter = 0;
        int minClicks = getMinClicks(context.getConfiguration());
        int currentMax = -1;
        String currentQuery = values.iterator().next().toString();
        String bestQuery = currentQuery;

        for (Text query : values) {
            String queryString = query.toString();
            if (!queryString.equals(currentQuery)) {
                if (queryCouter > currentMax) {
                    currentMax = queryCouter;
                    bestQuery = currentQuery;
                }
                currentQuery = queryString;
                queryCouter = 0;
            } else {
                queryCouter++;
            }
        }
        if (queryCouter > currentMax) {
            bestQuery = currentQuery;
            currentMax = queryCouter;
        }

        if ((currentMax + 1) >= minClicks)
            context.write(new Text(key.getHost() + "\t" + bestQuery), new IntWritable(currentMax + 1));
    }

    private static final String MINCLICKS = "seo.minclicks";

    public static int getMinClicks(Configuration conf) {
        return conf.getInt(MINCLICKS, 1);
    }

}
