import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by lihuaz on 4/22/17.
 */
public class KVParser {
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

    // part1------------------------------------------------------------------------
    public static class PMapper extends Mapper<Text, Text, Text, Text> {

        private String fileName = "";

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (key == null || value == null) {
                return;
            }
            fileName = key.toString();
            if (!fileName.matches("[0-9]+")) {
                return;
            }
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        // part1----------------------------------------
        Configuration conf1 = new Configuration();
        System.out.println("Parsing.");

        Job job1 = Job.getInstance(conf1, "TF");
        job1.setJarByClass(KVParser.class);

        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapperClass(PMapper.class);
        //job1.setReducerClass(TFReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
    }
}