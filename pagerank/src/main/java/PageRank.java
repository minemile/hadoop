import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class PageRank extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new PageRank(), args);
        System.exit(rc);
    }

    public int run(String[] args) throws Exception {
        String originalInput = args[0];
        String originalOutput = args[1];
        Job job = getJobConf(getConf(), args[0], args[1] + "/iter_0/", "0");
        if (job.waitForCompletion(true))
            System.out.println("Success init job");
        else
            return -1;

        for (int i = 1; i < 15; i++) {
            String input = "output/pagerank/iter_" + (i - 1) + "/" + "part-r-*";
            String output = originalOutput + "/iter_" + i;
            Job iterJob = getJobConf(getConf(), input, output, String.valueOf(i));
            if (iterJob.waitForCompletion(true))
                System.out.println("Success job iteration: " + i);
        }

        return 0;
    }

    private Job getJobConf(Configuration conf, String input, String output, String iterNum) throws IOException {
        conf.set("ITERNUM", iterNum);

        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRank.class);
        job.setJobName(PageRank.class.getCanonicalName() + "_" + iterNum);

        GraphInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setInputFormatClass(GraphInputFormat.class);

        job.setMapperClass(PageRank.PageRankMapper.class);
        job.setReducerClass(PageRank.PageRankReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GraphNode.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GraphNode.class);

        return job;
    }


    public static class PageRankMapper extends Mapper<LongWritable, GraphNode, Text, GraphNode> {

        @Override
        protected void map(LongWritable key, GraphNode value, Context context) throws IOException, InterruptedException {

            int iterNum = context.getConfiguration().getInt("ITERNUM", -1);
            if (iterNum == 0) {
                context.write(value.getUrl(), value);
                return;
            }

            context.write(value.getUrl(), value);

            String[] outLinks = value.getOutLinks();
            if (outLinks == null)
                return;

            float adjSize = outLinks.length;
            float p = value.getPageRank().get() / adjSize;
            for (int i = 0; i < outLinks.length; i++) {
                GraphNode outGraph = new GraphNode(i, outLinks[i], false, p);
                outGraph.setOutLinks("[]");
                context.write(new Text(outLinks[i]), outGraph);
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, GraphNode, Text, GraphNode> {
        private final static String LINK_COUNTER_PATH = "output/counter/part-r-00000"; // One reducer
        private long linkCounter = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path path = new Path(LINK_COUNTER_PATH);
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            FSDataInputStream inputStream = fs.open(path);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                linkCounter = Long.parseLong(br.readLine());
            }
        }

        @Override
        protected void reduce(Text key, Iterable<GraphNode> values, Context context) throws IOException, InterruptedException {
            int iterNum = context.getConfiguration().getInt("ITERNUM", -1);
            if (iterNum == 0) {
                GraphNode originalGN = values.iterator().next();
                originalGN.setPageRank(new FloatWritable(1f / linkCounter));
                context.write(originalGN.getUrl(), originalGN);
                return;
            }

            float newPageRank = 0;
            float outPageRank = 0;
            GraphNode newGraphNode = null;
            for (GraphNode graphNode : values) {
                if (!graphNode.getIsGraphNode().get()) {
                    outPageRank += graphNode.getPageRank().get();
                } else {
                    int id = graphNode.getId().get();
                    String url = graphNode.getUrl().toString();
                    newGraphNode = new GraphNode(id, url, true, graphNode.getPageRank().get());
                    newGraphNode.setOutLinks(graphNode.getRawOutLinks());
                }
            }
            if (newGraphNode == null) {
                System.out.println("Doens't have graph node");
                return;
            }

            newPageRank = 0.1f / linkCounter + 0.9f * outPageRank;
            newPageRank += newGraphNode.getPageRank().get();
            newGraphNode.setPageRank(new FloatWritable(newPageRank));
            context.write(newGraphNode.getUrl(), newGraphNode);
        }
    }


    public static class GraphInputFormat extends FileInputFormat<LongWritable, GraphNode> {

        @Override
        public RecordReader<LongWritable, GraphNode> createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {

            GraphNodeRecordReader graphNodeRecordReader = new GraphNodeRecordReader();
            graphNodeRecordReader.initialize(inputSplit, context);
            return graphNodeRecordReader;
        }
    }

    public static class GraphNodeRecordReader extends RecordReader<LongWritable, GraphNode> {

        LineRecordReader lineRecordReader;
        LongWritable key;
        GraphNode value;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(inputSplit, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!lineRecordReader.nextKeyValue()) {
                return false;
            }
            String line = lineRecordReader.getCurrentValue().toString();
            key = lineRecordReader.getCurrentKey();

            String[] rawValues = line.split("\t");

            int id = Integer.parseInt(rawValues[1]);
            String url = rawValues[2];
            String outLinks = rawValues[3];
            String inLinks = rawValues[4];
            boolean isGraphNode = Boolean.valueOf(rawValues[5]);
            float pagerank = Float.parseFloat(rawValues[6]);
            value = new GraphNode(id, url, isGraphNode, pagerank);
            value.setOutLinks(outLinks);
            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public GraphNode getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }
    }

}
