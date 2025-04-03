import java.io.IOException;
import java.util.ArrayList; // Added for storing valid tokens
import java.util.Iterator;
import java.util.List;      // Added for storing valid tokens
import java.util.regex.Matcher; // Added for regex
import java.util.regex.Pattern; // Added for regex

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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

public class HadoopWordStripes extends Configured implements Tool {

    // --- Stripe class remains unchanged ---
	private final static IntWritable one = new IntWritable(1); // Keep static one here

	public static class Stripe extends MapWritable {
		// Helper to add or increment a word count in the stripe
		public void add(String w) {
			Text key = new Text(w); // Use Text as key
			IntWritable currentCount = (IntWritable) get(key);
			if (currentCount == null) {
				put(key, new IntWritable(1)); // Use new IntWritable(1) instead of static 'one' to avoid issues if 'one' changes
			} else {
				currentCount.set(currentCount.get() + 1); // Increment existing count
                // No need to remove/put, just update the IntWritable object's value
			}
		}
        // --- merge and toString remain the same ---
	    public void merge(Stripe from) {
	        for (Writable fromKey : from.keySet()) {
                IntWritable fromValue = (IntWritable) from.get(fromKey);
                Text toKey = (Text) fromKey; // Assuming keys are Text
	            if (containsKey(toKey)) {
                    IntWritable toValue = (IntWritable) get(toKey);
                    toValue.set(toValue.get() + fromValue.get()); // Update existing count
                }
	            else {
	                put(toKey, new IntWritable(fromValue.get())); // Add new entry with a new IntWritable
                }
            }
	    }

	    @Override
	    public String toString() {
	        StringBuilder buffer = new StringBuilder();
	        buffer.append("{"); // Use braces for better map representation
            boolean first = true;
	        for (Writable key : keySet()) {
                if (!first) buffer.append(", ");
	            buffer.append(key).append("=").append(get(key)); // Use key=value format
                first = false;
            }
            buffer.append("}");
	        return buffer.toString();
	    }
	}


    public static class Map extends Mapper<LongWritable, Text, Text, Stripe> {

        // Pre-compile regex patterns for efficiency
        private static final Pattern WORD_PATTERN = Pattern.compile("^[a-z-]{6,24}$");
        private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?[0-9.]{4,16}$");
        private static final Pattern TOKEN_FINDER_PATTERN = Pattern.compile("[a-z0-9.-]+");

        private Text centerWord = new Text(); // Reusable Text object for the key

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

            // 4. Generate stripes from the list of *valid* tokens for this line
            for (int i = 0; i < validTokens.size(); i++) {
                String currentToken = validTokens.get(i);
                centerWord.set(currentToken); // Set the key (center word)

                Stripe stripe = new Stripe(); // Create a new stripe for this center word

                // Add previous valid token to stripe if it exists
                if (i > 0) {
                    String prevToken = validTokens.get(i - 1);
                    stripe.add(prevToken);
                }

                // Add next valid token to stripe if it exists
                if (i < validTokens.size() - 1) {
                    String nextToken = validTokens.get(i + 1);
                    stripe.add(nextToken);
                }

                // Emit the stripe only if it contains neighbors
                if (!stripe.isEmpty()) {
                   context.write(centerWord, stripe);
                }
            }
        }
    }

    // --- Reducer and Run method remain mostly unchanged (Reducer updated slightly for clarity) ---
	public static class Reduce extends Reducer<Text, Stripe, Text, Stripe> {
		@Override
		public void reduce(Text key, Iterable<Stripe> values, Context context)
				throws IOException, InterruptedException {

			Stripe globalStripe = new Stripe();

			for (Stripe localStripe : values) {
				globalStripe.merge(localStripe);
            }

            // Emit the final merged stripe for the key
            if (!globalStripe.isEmpty()) { // Optional: don't emit empty stripes if they somehow occur
			    context.write(key, globalStripe);
            }
		}
	}

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "HadoopWordStripesFiltered"); // Renamed Job
        job.setJarByClass(HadoopWordStripes.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Stripe.class); // Output value is Stripe

        job.setMapperClass(Map.class);
        // Combiner for stripes needs careful implementation - using the Reducer as combiner is correct here
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
        int ret = ToolRunner.run(new Configuration(), new HadoopWordStripes(), args);
        System.exit(ret);
    }
}
