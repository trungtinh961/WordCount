import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Join {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Join <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Join");
        job.setJarByClass(Join.class);

        job.setMapperClass(Mymapper.class);
        job.setReducerClass(Myreducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Mymapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String[] elements = value.toString().split(("\t"));
            if (filename.equals("forum_nodes_no_lf.tsv")) {
                if (elements.length > 5) {
                    if (!elements[0].equals("\"id\"")) {
                        String authorId = elements[3].substring(1, elements[3].length() - 1);
                        String score = elements[8].substring(1, elements[8].length() - 1);
                        context.write(new Text(authorId), new Text("score" + score));
                    }
                }
            }
            else {
                if (!elements[0].equals("\"user_ptr_id\"")) {
                    String authorId = elements[0].substring(1, elements[0].length() - 1);
                    String reputation = elements[1].substring(1, elements[1].length() - 1);
                    context.write(new Text(authorId), new Text("reput" + reputation));
                }
            }
        }
    }

    public static class Myreducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int score = 0;
            int reputation = 0;
            String authorId = key.toString();
            for (Text val : values) {
                if (val.toString().substring(0,5).equals("score")) {
                    score += Integer.parseInt(val.toString().substring(5));
                }
                else {
                    reputation = Integer.parseInt(val.toString().substring(5));
                }
            }
            context.write(new Text(authorId), new Text(reputation + "\t" + score));
        }
    }
}