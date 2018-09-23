import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Map<String, Boolean> stopWords = getStopWords();
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String sanitizedWord = getSanitizedWord(word.toString());
                if( !sanitizedWord.isEmpty() && !isStopWord(sanitizedWord, stopWords)) {
                    context.write(word, one);
                }
            }
        }

        private String getSanitizedWord(String word) {
            return word.replaceAll("[^A-Za-z]", "").toLowerCase();
        }

        private Boolean isStopWord(String word, Map<String, Boolean> stopWords) {
            return stopWords.getOrDefault(word, false);
        }

        private Map<String, Boolean> getStopWords() {
            String fileName = "stopWords.txt";
            Map<String, Boolean> stopWords = new HashMap<>();

            // File reading code taken from https://www.mkyong.com/java8/java-8-stream-read-a-file-line-by-line/
            //read file into stream, try-with-resources
            try (Stream<String> lines = Files.lines(Paths.get(fileName))) {
                lines.map(line -> stopWords.put(line, true));
            } catch (IOException e) {
                e.printStackTrace();
            }

            return stopWords;
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}