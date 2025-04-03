import java.io.IOException;
import java.util.ArrayList; // Added for storing valid tokens
import java.util.List;      // Added for storing valid tokens
import java.util.regex.Matcher; // Added for regex
import java.util.regex.Pattern; // Added for regex

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopWordPairs extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();
        // No longer need lastWord state across map calls for different lines if processing line by line

        // Pre-compile regex patterns for efficiency
        private static final Pattern WORD_PATTERN = Pattern.compile("^[a-z-]{6,24}$");
        private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?[0-9.]{4,16}$");
        private static final Pattern TOKEN_FINDER_PATTERN = Pattern.compile("[a-z0-9.-]+");


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase(); // 1. Convert to lowercase
            List<String> validTokens = new ArrayList<>(); // Store valid tokens for this line

            // 2. Find potential tokens
            Matcher matcher = TOKEN_FINDER_PATTERN.matcher(line);
            while (matcher.find()) {
                String token = matcher.group();

                // 3. Validate token
                boolean isValidWord = WORD_PATTERN.matcher(token).matches();
                boolean isValidNumber = NUMBER_PATTERN.matcher(token).matches()
                                       && token.matches("^-?[0-9.]+$"); // Ensure structure

                // Add to list if valid
                if (isValidWord || isValidNumber) {
                    validTokens.add(token);
                }
            }

            // 4. Generate pairs from the list of *valid* tokens for this line
            for (int i = 0; i < validTokens.size() - 1; i++) {
                String currentWord = validTokens.get(i);
                String nextWord = validTokens.get(i + 1);
                pair.set(currentWord + ":" + nextWord); // Use ':' as separator as in original
                context.write(pair, one);
            }
        }
    }

    // --- Reducer and Run method remain unchanged ---
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();
            context.write(key, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "HadoopWordPairsFiltered"); // Renamed Job
        job.setJarByClass(HadoopWordPairs.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class); // Assuming combiner is still desired
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopWordPairs(), args);
        System.exit(ret);
    }
}
