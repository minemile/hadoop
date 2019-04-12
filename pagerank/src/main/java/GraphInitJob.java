import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.zip.Inflater;

public class GraphInitJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new GraphInitJob(), args);
        System.exit(rc);
    }

    public int run(String[] args) throws Exception {
        Job job = getJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private Job getJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(GraphInitJob.class);
        job.setJobName(GraphInitJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(GraphInitMapper.class);
        job.setReducerClass(GraphInitReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GraphNode.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GraphNode.class);

        return job;
    }

    public static class GraphInitMapper extends Mapper<LongWritable, Text, Text, GraphNode> {
        private HashMap<Integer, String> docToUrl;
        private HashMap<String, Integer> urlToDoc;
        private final static String URLS_PATH = "data/urls.txt";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            docToUrl = new HashMap<>();
            urlToDoc = new HashMap<>();
            Path path = new Path(URLS_PATH);
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            FSDataInputStream inputStream = fs.open(path);
            String line;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                while ((line = br.readLine()) != null) {
                    String[] splitted = line.split("\t");
                    Integer id = Integer.parseInt(splitted[0]);
                    String url = splitted[1].trim();
                    if (url.endsWith("/")) url = url.substring(0, url.length() - 1);
                    docToUrl.put(id, url);
                    urlToDoc.put(url, id);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t");
            int documnetId = Integer.parseInt(splitted[0]);
            String docUrl = docToUrl.get(documnetId);
            String host = null;
            try {
                URI uri = new URI(docUrl).normalize();
                host = "http://" + uri.getHost();
            } catch (URISyntaxException e) {
                System.out.println("Error: " + e.getMessage());
                return;
            }

            byte[] bytesDocument = Base64.decodeBase64(splitted[1].getBytes());
            Inflater iflr = new Inflater();
            iflr.setInput(bytesDocument);
            byte[] tmp = new byte[4 * 1024];
            String html = null;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream(bytesDocument.length)) {
                while (!iflr.finished()) {
                    int size = iflr.inflate(tmp);
                    baos.write(tmp, 0, size);
                }
                html = baos.toString();
            } catch (Exception ex) {
                System.out.println("ERROR:" + ex.getMessage());
            }
            if (html == null) {
                context.getCounter("COMMON_COUNTERS", "BAD_HTML").increment(1);
                return;
            }

            Document doc = Jsoup.parse(html, host);
            HashSet<String> outHrefs = new HashSet<>();
            Elements links = doc.select("a[href]");
            int i = 0;
            for (Element link : links) {
                String linkHref = link.attr("abs:href"); // Normalize
                if (linkHref.endsWith("/")) linkHref = linkHref.substring(0, linkHref.length() - 1);


                String normalizedHref;
                try {
                    URI uri = new URI(linkHref).normalize();
                    normalizedHref = uri.toString().replace(",", ".").trim();
                } catch (URISyntaxException e) {
                    System.out.println("ERROR: " + e.getMessage());
                    continue;
                }

                if (!normalizedHref.startsWith("http:")) continue;
                outHrefs.add(normalizedHref);
                if (!urlToDoc.containsKey(normalizedHref)) {
                    // So it doesn't have out urls
                    GraphNode gnNoOutLinks = new GraphNode(i, normalizedHref, true, 1f);
                    gnNoOutLinks.setOutLinks("[]");
                    context.write(new Text(normalizedHref), gnNoOutLinks);
                }
                i++;
            }
            GraphNode gn = new GraphNode(documnetId, docUrl, true, 1f);
            gn.setOutLinks(outHrefs.toString());
            context.write(new Text(docUrl), gn);
        }
    }


    public static class GraphInitReducer extends Reducer<Text, GraphNode, Text, GraphNode> {


        @Override
        protected void reduce(Text key, Iterable<GraphNode> values, Context context) throws IOException, InterruptedException {
            GraphNode gf = values.iterator().next();
//            gf.setPageRank(new FloatWritable(1f / linkCounter));
            context.write(new Text(gf.getUrl().toString()), gf);
        }
    }
}
