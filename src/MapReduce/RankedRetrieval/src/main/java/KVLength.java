import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by lihuaz on 4/24/17.
 */
public class KVLength {
    private static Set<String> reutersStopWords;

    static {
        reutersStopWords = new HashSet<String>();
        reutersStopWords.add("a");
        reutersStopWords.add("an");
        reutersStopWords.add("and");
        reutersStopWords.add("are");
        reutersStopWords.add("as");
        reutersStopWords.add("at");
        reutersStopWords.add("be");
        reutersStopWords.add("by");
        reutersStopWords.add("for");
        reutersStopWords.add("from");
        reutersStopWords.add("has");
        reutersStopWords.add("he");
        reutersStopWords.add("in");
        reutersStopWords.add("is");
        reutersStopWords.add("it");
        reutersStopWords.add("its");
        reutersStopWords.add("of");
        reutersStopWords.add("on");
        reutersStopWords.add("that");
        reutersStopWords.add("the");
        reutersStopWords.add("to");
        reutersStopWords.add("was");
        reutersStopWords.add("were");
        reutersStopWords.add("will");
        reutersStopWords.add("with");
    }

    public static class LenMapper extends Mapper<Text, Text, Text, Text> {
        private int length;
        private String fileName;

        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (key == null || value == null) {
                return;
            }

            length = 0;
            fileName = key.toString();
            if (!fileName.matches("[0-9]+")) {
                return;
            }
            String valString = value.toString().replaceAll("[^a-zA-Z]+", " ");
            StringTokenizer tokenizer = new StringTokenizer(valString);
            while (tokenizer.hasMoreTokens()) {
                String currToken = tokenizer.nextToken().toLowerCase();
                if (reutersStopWords.contains(currToken)) {
                    continue;
                } else {
                    length++;
                }
            }
            context.write(new Text(fileName), new Text(String.valueOf(length)));
        }
    }

    public static class LenReducer extends Reducer<Text, Text, Text, Text> {
        private long totalLen = 0;

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int currLen = Integer.parseInt(values.iterator().next().toString());
            totalLen += currLen;
            context.write(key, new Text(String.valueOf(currLen)));
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Avg length: "), new Text(String.valueOf(totalLen/806791)));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf1 = new Configuration();
        System.out.println("Calculating Document Length.");

        Job job1 = Job.getInstance(conf1, "TF");
        job1.setJarByClass(KVLength.class);

        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapperClass(LenMapper.class);
        job1.setReducerClass(LenReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
    }
}
