import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoinChain {

    public static class CustsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString().trim();
            String[] parts = record.split(",");
            String age = parts[3];
            String id = parts[0];

            context.write(new Text(id), new Text("age," + age));
        }
    }

    public static class TxnsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString().trim();
            String[] parts = record.split(",");

            String gametype = parts[4];
            String id = parts[2];

            context.write(new Text(id), new Text("type," + gametype));
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
        private Map<Integer, Set<String>> ageToGameTypes = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

        	 Set<String> gameTypes = new HashSet<>();
        	    Integer age = null;

        	    // Process each value to extract the game type and age
        	    for (Text value : values) {
        	        String[] parts = value.toString().split(",");
        	        if (parts[0].equals("type")) {
        	            gameTypes.add(parts[1]); 
        	        } else if (parts[0].equals("age")) {
        	            age = Integer.parseInt(parts[1]);
        	        }
        	    }

            // Store game types under the respective age
            if (age != null) {
                if (!ageToGameTypes.containsKey(age)) {
                    ageToGameTypes.put(age, new HashSet<String>());
                }
                ageToGameTypes.get(age).addAll(gameTypes); S
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, Set<String>> entry : ageToGameTypes.entrySet()) {
                Integer age = entry.getKey();
                Set<String> gameTypes = entry.getValue();

                StringBuilder gameTypesStringBuilder = new StringBuilder();
                for (String gameType : gameTypes) {
                    if (gameTypesStringBuilder.length() > 0) {
                        gameTypesStringBuilder.append(",");
                    }
                    gameTypesStringBuilder.append(gameType);
                }

                context.write(new Text(age.toString()), new Text(gameTypesStringBuilder.toString()));
            }
        }
    }

    public static class GameTypeStatMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString().trim();
            String[] parts = record.toString().split("\t");

            if (parts.length == 2) {
                String age = parts[0];
                String gameTypes = parts[1];
                for (String gameType : gameTypes.split(",")) {
                    context.write(new Text(gameType), new Text(age));
                }
            }
        }
    }

    public static class GameTypeStatReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, Double> averageAges = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String gameType = key.toString();
            List<Integer> ages = new ArrayList<>();

            for (Text value : values) {
                ages.add(Integer.parseInt(value.toString()));
            }
            if (ages.isEmpty()) {
                return;
            }
            int minAge = Integer.MAX_VALUE;
            int maxAge = Integer.MIN_VALUE;
            int sumAge = 0;

            for (int age : ages) {
                if (age < minAge) {
                    minAge = age;
                }
                if (age > maxAge) {
                    maxAge = age;
                }
                sumAge += age;
            }

            double avgAge = (double) sumAge / ages.size();
            averageAges.put(gameType, avgAge);

            String result = String.format("Min: %d Max: %d Avg: %.2f", minAge, maxAge, avgAge);
            context.write(new Text(gameType), new Text(result));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Find the game type with the lowest average age
            StringBuilder minAvgGameTypes = new StringBuilder();
            double minAvgAge = Double.MAX_VALUE;

            for (Map.Entry<String, Double> entry : averageAges.entrySet()) {
                double avg = entry.getValue();
                if (avg < minAvgAge) {
                    minAvgAge = avg;
                    minAvgGameTypes.setLength(0); // Clear previous entries
                    minAvgGameTypes.append(entry.getKey());
                } else if (avg == minAvgAge) {
                    // Append game type if it has the same average age
                    minAvgGameTypes.append(", ").append(entry.getKey());
                }
            }

            // Write the game type with the lowest average age to the context
            if (minAvgGameTypes.length() > 0) {
                context.write(
                        new Text("Game types with lowest average age of " + String.format("%.1f", minAvgAge) + ":"),
                        new Text(minAvgGameTypes.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Reduce side join job 1");

        job.setJarByClass(ReduceJoinChain.class);

        job.setReducerClass(ReduceJoinReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustsMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TxnsMapper.class);

        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);

        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "Reduce side join job 2");

        job2.setJarByClass(ReduceJoinChain.class);

        job2.setMapperClass(GameTypeStatMapper.class);

        job2.setReducerClass(GameTypeStatReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(args[2]));

        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}