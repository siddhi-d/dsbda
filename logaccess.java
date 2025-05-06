
log access
 
mapper

package finalsiddhilog;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class log_map extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        if (words.length > 0) {
   context.write(new Text(words[0]), new IntWritable(1));  // Emit first word
        }
    }
}




reducer

package finalsiddhilog;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class log_reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    Text maxWord = new Text();
    int max = 0;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        if (sum > max) {
            max = sum;
            maxWord.set(key);
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(maxWord, new IntWritable(max));  // Emit only most frequent word
    }
}







 driver

package finalsiddhilog;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;




public class log_driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 2) {
            System.err.println("Usage: sid_logdriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log Count");

        job.setJarByClass(log_driver.class);
        job.setMapperClass(log_map.class);
        job.setReducerClass(log_reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);  // Important to ensure max is global

        job.waitForCompletion(true);
    }
}





