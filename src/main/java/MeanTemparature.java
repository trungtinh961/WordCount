import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MeanTemparature {
    public static class Mymapper extends Mapper<Object, Text, IntWritable,Text> {
        public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] elements = line.split(",");
            int month=Integer.parseInt(elements[1].substring(4,6));
            Text data=new Text(elements[3]);
            context.write(new IntWritable(month), data);
        }
    }

    public static class Myreducer extends  Reducer<IntWritable,Text,IntWritable,IntWritable> {
        public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
            int temp=0;
            int max=0;
            for(Text t:values)
            {
                String tmp = t.toString().trim();
                if(!tmp.equals("")) {
                    temp=Integer.parseInt(tmp);
                    if (temp > max) {
                        max = temp;
                    }
                }
            }
            context.write(key,new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mean temparature");
        job.setJarByClass(MeanTemparature.class);
        job.setMapperClass(Mymapper.class);
        job.setReducerClass(Myreducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }
}