import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class L2Norm {

    public static class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

      /*
      * Mapper class.
      * -------------
      * Basic mapper, emitting (column_index j => squared coefficient r[j] * r[j]) for each row.
      */

        private IntWritable index = new IntWritable();
        private DoubleWritable product  = new DoubleWritable();

        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {

            // Current row (r_i).
            String[] r = value.toString().split("\\s+");

            for (int j=0; j<r.length; j++){

                Double r_j = Double.parseDouble(r[j]);
                product.set(r_j * r_j);
                index.set(j);
                context.write(index, product);

            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

      /*
      * Reducer class.
      * -------------
      * Basic summation reducer.
      * MultipleOutputs is used to output a file in the form : "l2-*".
      */

        private MultipleOutputs<IntWritable, DoubleWritable> multipleOutputs;

        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }

            DoubleWritable sqrt_sum = new DoubleWritable(Math.sqrt(sum));

            // Previously :
            //context.write(key, new DoubleWritable(sum));

            String filename = "l2";
            multipleOutputs.write(key, sqrt_sum, filename);
        }

        @Override
        public void setup(Context context){
            multipleOutputs = new MultipleOutputs<IntWritable, DoubleWritable>(context);
        }

        @Override
        public void cleanup(final Context context) throws IOException, InterruptedException{
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "L2Norm");

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);

        // Previous output format : part-*
        // job.setOutputFormatClass(TextOutputFormat.class);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
