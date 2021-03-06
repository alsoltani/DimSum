import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DimSum {

    /**
     * Mapper class.
     * -------------
     * For all a_ij in row i,
     *  with probability min(1, sqrt(gamma)/|cj|),
     *      for all a_ik in row i,
     *          with probability min(1, sqrt(gamma)/|ck|),
     *              emit ((j, k) => a_ij * a_ik / (min(sqrt(gamma), |cj|) * min(sqrt(gamma), |ck|)).
     */

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text pair = new Text();
        private DoubleWritable product  = new DoubleWritable();

        //TODO: pass similarity_threshold into the DimSum class constructor.

        Double similarity_threshold = 0.01;

        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {

            // Load L2-norms from localhost.
            // ----------------------------
            // To run the job locally :      "hdfs://localhost:9000//..."
            // To run the job on a cluster : "hdfs:/..."

            List<Double> norm = new ArrayList<Double>();

            Path pt = new Path("hdfs://localhost:9000//user/hadoop/l2norm/output/l2-r-00000");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

            try {
                String line;
                line = br.readLine();
                while (line != null){

                    String[] row = line.split("\\s+");
                    norm.add(Double.parseDouble(row[1]));

                    // Be sure to read the next line; otherwise you'll get an infinite loop.
                    line = br.readLine();
                }
            } finally {

                // Close out the BufferedReader.
                br.close();
            }

            // Actual Mapper implementation
            // ----------------------------

            Double sqrt_gamma = Math.sqrt(4.0 * Math.log(norm.size()) / similarity_threshold);
            Random rand = new Random();

            // Current row (r_i).

            String[] r = value.toString().split("\\s+");

            for (int j=0; j<r.length; j++){

                // Compute probability p_j.

                Double prob_j = Math.min(1.0, sqrt_gamma / norm.get(j));

                if (rand.nextDouble() < prob_j){

                    for (int k=0; k<r.length; k++){

                        // Compute probability p_k.

                        Double prob_k = Math.min(1.0, sqrt_gamma / norm.get(k));

                        if (rand.nextDouble() < prob_k){

                            // Convert a_ij = r[j], and a_jk = r[k] to float.

                            Double r_j = Double.parseDouble(r[j]);
                            Double r_k = Double.parseDouble(r[k]);

                            product.set(r_j * r_k /
                                    (Math.min(sqrt_gamma, norm.get(j)) * Math.min(sqrt_gamma, norm.get(k))));
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

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            Double sum = 0.0;
            for (DoubleWritable val : values) {
                if (val.get() > 0.0) {
                    sum += val.get();
                }
            }

            context.write(key, new DoubleWritable(sum));
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
        Job job = Job.getInstance(conf, "DimSum");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}