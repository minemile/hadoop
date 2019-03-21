import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.net.URI;
import java.io.IOException;
import java.net.URISyntaxException;

public class SeoMapper extends Mapper<LongWritable, Text, HostUrlPair, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String query = line[0];
        String host = "";
        try {
            URI url = new URI(line[1]);
            host = url.getHost();
            if (host == null) host = "";
        } catch (URISyntaxException e) {
            System.out.println("Error: " + e.getMessage());
        }
        HostUrlPair hup = new HostUrlPair(host, query);
        context.write(hup, new Text(query));
    }
}
