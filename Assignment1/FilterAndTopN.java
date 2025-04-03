import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FilterAndTopN extends Configured implements Tool {

    private static final int TOP_N = 100;

    // Helper class remains the same
    private static class CountedItem implements Comparable<CountedItem> {
        final int count;
        final String item;

        CountedItem(int count, String item) {
            this.count = count;
            this.item = item;
        }

        @Override
        public int compareTo(CountedItem other) {
            int cmp = Integer.compare(this.count, other.count);
            if (cmp == 0) {
                cmp = this.item.compareTo(other.item);
            }
            return cmp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CountedItem that = (CountedItem) o;
            return count == that.count && item.equals(that.item);
        }

        @Override
        public int hashCode() {
            return 31 * item.hashCode() + count;
        }
    }

    // Mapper class remains the same
    public static class FilterTopNMapper extends Mapper<Text, Text, Text, Text> {
        private PriorityQueue<CountedItem> topWords = new PriorityQueue<>(TOP_N + 1);
        private PriorityQueue<CountedItem> topNumWordPairs = new PriorityQueue<>(TOP_N + 1);
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String keyString = key.toString();
            int count;
            try {
                 count = Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid count for key " + keyString + ": " + value.toString());
                return;
            }

            String item;
            String prefix;
            int firstColon = keyString.indexOf(':');
            if (firstColon <= 0 || firstColon + 1 >= keyString.length()) {
                 System.err.println("Skipping malformed key (missing or misplaced ':'): " + keyString);
                 return;
            }
            prefix = keyString.substring(0, firstColon);
            item = keyString.substring(firstColon + 1);

            if (count == 1000) {
                if ("W".equals(prefix)) {
                    outputKey.set("EXACT_W");
                    outputValue.set(item + "\t" + count);
                    context.write(outputKey, outputValue);
                } else if ("WW".equals(prefix)) {
                    outputKey.set("EXACT_WW");
                    outputValue.set(item + "\t" + count);
                    context.write(outputKey, outputValue);
                }
            }

            CountedItem currentItem = new CountedItem(count, item);
            if ("W".equals(prefix)) {
                topWords.add(currentItem);
                if (topWords.size() > TOP_N) {
                    topWords.poll();
                }
            } else if ("NW".equals(prefix)) {
                topNumWordPairs.add(currentItem);
                if (topNumWordPairs.size() > TOP_N) {
                    topNumWordPairs.poll();
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!topWords.isEmpty()) {
                CountedItem item = topWords.poll();
                outputKey.set("TOP_W");
                outputValue.set(item.count + "\t" + item.item);
                context.write(outputKey, outputValue);
            }
            while (!topNumWordPairs.isEmpty()) {
                CountedItem item = topNumWordPairs.poll();
                outputKey.set("TOP_NW");
                outputValue.set(item.count + "\t" + item.item);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class FilterTopNReducer extends Reducer<Text, Text, NullWritable, Text> {
        private PriorityQueue<CountedItem> topWordsHeap = new PriorityQueue<>(TOP_N + 1);
        private PriorityQueue<CountedItem> topNumWordPairsHeap = new PriorityQueue<>(TOP_N + 1);
        private TreeSet<CountedItem> finalTopWords = new TreeSet<>(Comparator.comparingInt((CountedItem ci) -> ci.count)
                                                                             .thenComparing(ci -> ci.item).reversed());
        private TreeSet<CountedItem> finalTopNumWordPairs = new TreeSet<>(Comparator.comparingInt((CountedItem ci) -> ci.count)
                                                                                    .thenComparing(ci -> ci.item).reversed());
        private MultipleOutputs<NullWritable, Text> mos;
        private Text outputValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String keyPrefix = key.toString();

            if (keyPrefix.startsWith("EXACT_")) {
                // ** CORRECTED BASE PATHS using only letters **
                String baseOutputPath = keyPrefix.equals("EXACT_W") ? "exactwords/part" : "exactwwpairs/part";
                for (Text val : values) {
                    mos.write(NullWritable.get(), val, baseOutputPath);
                }
            } else if (keyPrefix.startsWith("TOP_")) {
                PriorityQueue<CountedItem> targetHeap = keyPrefix.equals("TOP_W") ? topWordsHeap : topNumWordPairsHeap;
                for (Text val : values) {
                    String[] parts = val.toString().split("\t", 2);
                    if (parts.length == 2) {
                        try {
                            int count = Integer.parseInt(parts[0]);
                            String item = parts[1];
                            targetHeap.add(new CountedItem(count, item));
                            if (targetHeap.size() > TOP_N) {
                                targetHeap.poll();
                            }
                        } catch (NumberFormatException e) {
                             System.err.println("Reducer skipping malformed count in value for key " + keyPrefix + ": " + val.toString());
                        }
                    } else {
                        System.err.println("Reducer skipping malformed value (missing tab) for key " + keyPrefix + ": " + val.toString());
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
             while (!topWordsHeap.isEmpty()) {
                 finalTopWords.add(topWordsHeap.poll());
             }
             while (!topNumWordPairsHeap.isEmpty()) {
                 finalTopNumWordPairs.add(topNumWordPairsHeap.poll());
             }

            // ** CORRECTED BASE PATHS using only letters **
            String topWordsPath = "topwords/part";
            for (CountedItem item : finalTopWords) {
                 outputValue.set(item.item + "\t" + item.count);
                 mos.write(NullWritable.get(), outputValue, topWordsPath);
            }

            // ** CORRECTED BASE PATHS using only letters **
            String topNwPath = "topnwpairs/part";
            for (CountedItem item : finalTopNumWordPairs) {
                 outputValue.set(item.item + "\t" + item.count);
                 mos.write(NullWritable.get(), outputValue, topNwPath);
            }

            mos.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
         if (args.length != 2) {
            System.err.println("Usage: FilterAndTopN <job1_output_path> <final_output_base_path>");
            return -1;
        }

        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        Job job = Job.getInstance(conf, "Filter and Top N");
        job.setJarByClass(FilterAndTopN.class);

        job.setMapperClass(FilterTopNMapper.class);
        job.setReducerClass(FilterTopNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Configure MultipleOutputs - ** CORRECTED NAMES using only letters **
        MultipleOutputs.addNamedOutput(job, "exactwords", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "exactwwpairs", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "topwords", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "topnwpairs", TextOutputFormat.class, NullWritable.class, Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

     public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FilterAndTopN(), args);
        System.exit(exitCode);
    }
}
