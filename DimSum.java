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
     * In its original form :
     * For all pairs (a_ij, a_ik) in row i,
     * with probability min(1, gamma/(|cj||ck|)),
     * emit ((cj, ck), a_ij * a_ik).
     */

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text pair = new Text();
        private DoubleWritable product  = new DoubleWritable();

        //TODO: pass similarity_threshold into the DimSum class constructor.

        Double similarity_threshold = 0.1;

        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {

            // Load L2-norms from localhost.
            // TODO: Use generic filename in Scanner.
            // ----------------------------

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

            // Current row (r_i).

            String[] r = value.toString().split("\\s+");

            for (int j=0; j<r.length; j++){

                // Compute probability p_j.

                Double prob_j = Math.min(1.0, sqrt_gamma / norm.get(j));
                Random rand = new Random();

                if (rand.nextFloat() < prob_j){

                    for (int k=0; k<r.length; k++){

                        // Compute probability p_k.

                        Double prob_k = Math.min(1.0, sqrt_gamma / norm.get(k));

                        if (rand.nextFloat() < prob_k){

                            // Convert a_ij = r[j], and a_jk = r[k] to float.

                            float r_j = Float.parseFloat(r[j]);
                            float r_k = Float.parseFloat(r[k]);

                            product.set(r_j * r_k /
                                    (Math.min(sqrt_gamma, norm.get(j)) * Math.min(sqrt_gamma, norm.get(k))));
                            pair.set(String.valueOf(j) + "_" + String.valueOf(k));
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

            float sum  = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
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