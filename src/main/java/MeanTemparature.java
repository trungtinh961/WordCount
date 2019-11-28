import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MeanTemparature {
    public static class Mymapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elements = line.split(",");
            context.write(new Text(elements[0].substring(2)), new Text(elements[2]));
        }
    }

//        public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
//            String line = value.toString();
//            String[] elements = line.split(",");
//            String monthyear = elements[0].substring(2,8);
//            Float data = Float.parseFloat(elements[2]);
//            context.write(new Text(monthyear), new FloatWritable(data));
//        }

//    public  static class Mycombiner extends Reducer<Text, FloatWritable, Text,FloatWritable> {
//        private FloatWritable result = new FloatWritable();
//        public void reduce (Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
//            int sum = 0; int count = 0;
//            for (FloatWritable val : values) {
//                sum += val.get();
//                count++;
//            }
//            result.set(sum);
//            context.write(key,(result,count));
//        }
//    }

    public static class Myreducer extends  Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double average = 0d;
            int count = 0;
            Double sum = 0d;
            for (Text val : values) {
                Double temp = Double.parseDouble(val.toString().trim());
                sum += temp;
                count++;
            }
            average = sum / count;
            result.set(average);
            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mean temparature");
        job.setJarByClass(MeanTemparature.class);
        job.setMapperClass(Mymapper.class);
        job.setReducerClass(Myreducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }
}