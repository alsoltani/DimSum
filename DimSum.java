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

public class DimSum {

  /**
   * Mapper class.
   * -------------
   * In its original form :
   * For all pairs (a_ij, a_ik) in row i,
   * with probability min(1, gamma/(|cj||ck|)),
   * emit ((cj, ck), a_ij * a_ik).
   */

  //TODO: Compute sqrt_gamma, list of matrix norms norm_c.

  public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

      private final static FloatWritable one  = new FloatWritable(1);
      private FloatWritable product  = new FloatWritable();
      private Text pair = new Text();

      public void map(LongWritable key, Text value,
                      Context context) throws IOException, InterruptedException {

          // Current row (r_i).
          String[] r = value.toString().split("\\s");

          for (int j=0; j<r.length; j++){

              // Compute probability p_j.
              float prob_j = Math.min(one, sqrt_gamma / norm_c[j]);
              Random rand = new Random();

              if (rand.nextFloat() < prob_j){

                  for (int k=0; k<r.length; k++){

                      // Compute probability p_k.
                      float prob_k = Math.min(one, sqrt_gamma / norm_c[k]);

                      if (rand.nextFloat() < prob_k){

                          // Convert a_ij = r[j], and a_jk = r[k] to float.
                          float r_j = Float.parseFloat(r[j]);
                          float r_k = Float.parseFloat(r[k]);

                          product.set(r_j * r_k / (Math.min(sqrt_gamma, c[j]) * Math.min(sqrt_gamma, c[k])));
                          pair.set(String.valueOf(j) + "\t" + String.valueOf(k));
                          context.write(pair, product);
                      }
                  }
              }
          }
      }
  }

  /**
   * Reducer class.
   * -------------
   * A reducer class that just emits the sum of the input values.
   */

  public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        float sum  = 0;
        for (FloatWritable val : values) {
            sum += val.get();
        }
        context.write(key, new FloatWritable(sum));
    }
  }

  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the
   * job tracker.
   */

  public static void main(String[] args) throws Exception {

      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Dim Sum Algorithm");

      job.setOutputKeyClass(Text.class);
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