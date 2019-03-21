import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SeoJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new SeoJob(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    Job GetJobConf(Configuration conf, String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(SeoJob.class);
        job.setJobName(SeoJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setMapperClass(SeoMapper.class);
        job.setReducerClass(SeoReducer.class);

        job.setPartitionerClass(SeoJob.HostPartitioner.class);
        job.setSortComparatorClass(SeoJob.KeyComparator.class);
        job.setGroupingComparatorClass(SeoJob.HostGrouper.class);

//        job.setNumReduceTasks(128);
        job.setMapOutputKeyClass(HostUrlPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    public static class HostGrouper extends WritableComparator {
        protected HostGrouper() {
            super(HostUrlPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // считаем за группу текстовую часть ключа: {id станции, день, месяц}
            Text a_first = ((HostUrlPair)a).getHost();
            Text b_first = ((HostUrlPair)b).getHost();
            return a_first.compareTo(b_first);
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(HostUrlPair.class, true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((HostUrlPair)a).compareTo((HostUrlPair)b);
        }
    }

    public static class HostPartitioner extends Partitioner<HostUrlPair, Text> {
        @Override
        public int getPartition(HostUrlPair key, Text val, int numPartitions) {
            return Math.abs(key.getHost().hashCode()) % numPartitions;
        }
    }
}
