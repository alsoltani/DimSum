package org.apache.hadoop.examples;

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

public class L2Norm {

  public static class Map extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

      private IntWritable index = new IntWritable();
      private FloatWritable product  = new FloatWritable();

      public void map(LongWritable key, Text value,
                      Context context) throws IOException, InterruptedException {

          // Current row (r_i).
          String[] r = value.toString().split("\\t");

          for (int j=0; j<r.length; j++){

              float r_j = Float.parseFloat(r[j]);
              product.set(r_j * r_j);
              index.set(j);
              context.write(index, product);

          }
      }
  }

  public static class Reduce extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        float sum  = 0;
        for (FloatWritable val : values) {
            sum += val.get();
        }
        context.write(key, new FloatWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {

      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "L2Norm");

      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(FloatWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
  }
}
