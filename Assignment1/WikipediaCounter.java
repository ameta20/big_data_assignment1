import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path; // Required for Path class
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

// Import the Pair class if it's defined as an inner class in TokenMapper
// Or make Pair a top-level or static nested class if preferred.
// Assuming Pair is defined within TokenMapper for now.


public class WikipediaCounter extends Configured implements Tool {

    // Enum and validation logic remain the same
    private enum TokenType { WORD, NUMBER, INVALID }
    private static final Pattern WORD_PATTERN = Pattern.compile("^[a-z-]{6,24}$");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?[0-9.]{4,16}$");
    private static final Pattern NUMBER_STRUCTURE_PATTERN = Pattern.compile("^-?[0-9.]+$");
    private static final Pattern TOKEN_FINDER_PATTERN = Pattern.compile("[a-z0-9.-]+");

    private static TokenType getTokenType(String token) {
        if (WORD_PATTERN.matcher(token).matches()) {
            return TokenType.WORD;
        } else if (NUMBER_PATTERN.matcher(token).matches() && NUMBER_STRUCTURE_PATTERN.matcher(token).matches()) {
            return TokenType.NUMBER;
        } else {
            return TokenType.INVALID;
        }
    }

    // TokenMapper class remains the same
    public static class TokenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();

        // Define Pair class here or make it accessible otherwise
        private static class Pair<T1, T2> {
            final T1 token;
            final T2 type;
            Pair(T1 token, T2 type) { this.token = token; this.type = type; }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            List<Pair<String, TokenType>> validTokens = new ArrayList<>();

            Matcher matcher = TOKEN_FINDER_PATTERN.matcher(line);
            while (matcher.find()) {
                String token = matcher.group();
                TokenType type = getTokenType(token);
                if (type != TokenType.INVALID) {
                    validTokens.add(new Pair<>(token, type));
                }
            }

            for (int i = 0; i < validTokens.size(); i++) {
                Pair<String, TokenType> current = validTokens.get(i);

                if (current.type == TokenType.WORD) {
                    outputKey.set("W:" + current.token);
                    context.write(outputKey, one);
                }

                if (i + 1 < validTokens.size()) {
                    Pair<String, TokenType> next1 = validTokens.get(i + 1);
                    emitPair(current, next1, context);
                }
                if (i + 2 < validTokens.size()) {
                    Pair<String, TokenType> next2 = validTokens.get(i + 2);
                    emitPair(current, next2, context);
                }
            }
        }

        private void emitPair(Pair<String, TokenType> token1, Pair<String, TokenType> token2, Context context)
                throws IOException, InterruptedException {
            String prefix = "";
            if (token1.type == TokenType.WORD && token2.type == TokenType.WORD) {
                prefix = "WW:";
            } else if (token1.type == TokenType.NUMBER && token2.type == TokenType.WORD) {
                prefix = "NW:";
            } else if (token1.type == TokenType.WORD && token2.type == TokenType.NUMBER) {
                prefix = "WN:";
            } else if (token1.type == TokenType.NUMBER && token2.type == TokenType.NUMBER) {
                prefix = "NN:";
            } else {
                return;
            }
            outputKey.set(prefix + token1.token + ":" + token2.token);
            context.write(outputKey, one);
        }
    }

    // SumReducer class remains the same
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // *** MODIFIED ARGUMENT HANDLING AS PER YOUR SUGGESTION ***
        if (args.length < 2) { // Need at least one input and one output
            System.err.println("Usage: WikipediaCounter <input_path_1> [<input_path_2> ...] <output_path>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Wikipedia Counter");
        job.setJarByClass(WikipediaCounter.class);

        job.setMapperClass(TokenMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set multiple input paths from command line arguments
        // Assumes all args except the last one are input paths
        Path[] inputPaths = new Path[args.length - 1];
        System.out.println("Setting input paths:");
        for (int i = 0; i < args.length - 1; i++) {
            inputPaths[i] = new Path(args[i]);
            System.out.println("  " + args[i]); // Log path being added
        }
        FileInputFormat.setInputPaths(job, inputPaths);

        // Output path is the last argument
        Path outputPath = new Path(args[args.length - 1]);
        System.out.println("Setting output path:");
        System.out.println("  " + outputPath.toString());
        FileOutputFormat.setOutputPath(job, outputPath);
        // *** END OF MODIFIED ARGUMENT HANDLING ***

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WikipediaCounter(), args);
        System.exit(exitCode);
    }
}
