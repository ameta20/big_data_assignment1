import java.io.IOException;
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

public class HadoopWordCount extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text wordToken = new Text(); // Renamed for clarity

        // Pre-compile regex patterns for efficiency
        // Pattern 1: Lower-case words with '-' (length 6-24)
        private static final Pattern WORD_PATTERN = Pattern.compile("^[a-z-]{6,24}$");
        // Pattern 2: Numbers with '.', optional leading '-' (length 4-16)
        private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?[0-9.]{4,16}$");
        // Pattern to find potential tokens (sequences of letters, digits, -, .)
        private static final Pattern TOKEN_FINDER_PATTERN = Pattern.compile("[a-z0-9.-]+");


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase(); // 1. Convert to lowercase

            // 2. Find potential tokens using TOKEN_FINDER_PATTERN
            Matcher matcher = TOKEN_FINDER_PATTERN.matcher(line);

            while (matcher.find()) {
                String token = matcher.group(); // Get the matched token

                // 3. Validate token against rules
                boolean isValidWord = WORD_PATTERN.matcher(token).matches();
                boolean isValidNumber = NUMBER_PATTERN.matcher(token).matches()
                                       // Additional check for number format (only digits and dots after optional '-')
                                       && token.matches("^-?[0-9.]+$");

                // 4. Emit only if it matches one of the patterns
                if (isValidWord || isValidNumber) {
                    wordToken.set(token);
                    context.write(wordToken, one);
                }
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
        Job job = Job.getInstance(new Configuration(), "HadoopWordCountFiltered"); // Renamed Job
        job.setJarByClass(HadoopWordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopWordCount(), args);
        System.exit(ret);
    }
}
