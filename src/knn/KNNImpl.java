package knn;

import distances.Distance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import writables.Point;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class KNNImpl {

    public static class KnnMapper extends Mapper<Object, Text, Text, PointWithDistAndLabel> {
        private Point point;
        private TestSetIterator testSetIterator;

        @Override
        protected void setup(Context context) throws IOException {
            point = new Point();
            testSetIterator = new TestSetIterator(Distance.getDistance(context.getConfiguration().get("distance")));

            List<URI> uris = Arrays.asList(context.getCacheFiles());
            FileSystem fs = FileSystem.get(context.getConfiguration());
            InputStreamReader ir = new InputStreamReader(fs.open(new Path(uris.get(0))));
            BufferedReader br = new BufferedReader(ir);

            String line = br.readLine();
            while (line != null) {
                testSetIterator.add(line);
                line = br.readLine();
            }

        }

        @Override
        public void map(Object key, Text value, Context context) {
            String[] splits = value.toString().split(",");

            //Parse point and its label
            point.parse(splits[0]);
            String label = splits[1];

            //Find distance of given point from all points in the test dataset
            try {
                List<PointWithDistAndLabel> result = testSetIterator.getDistFromAllTestSet(point, label);
                for (PointWithDistAndLabel pointWithDistAndLabel : result)
                    context.write(new Text(pointWithDistAndLabel.getFrom().getDataLabel()), pointWithDistAndLabel);
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }

    public static class KnnReducer extends Reducer<Text, PointWithDistAndLabel, Text, NullWritable> {
        Integer K;
        List<PointWithDistAndLabel> kNearest = new ArrayList<>();

        @Override
        protected void setup(Context context) {
            K = context.getConfiguration().getInt("K", 3);
        }

        @Override
        public void reduce(Text key, Iterable<PointWithDistAndLabel> values, Context context) throws IOException, InterruptedException {
            List<PointWithDistAndLabel> points = new ArrayList<>();
            for (PointWithDistAndLabel value : values) {
                PointWithDistAndLabel p = new PointWithDistAndLabel(value);
                points.add(p);
            }

            //Sort List
            points.sort(new Comparator<PointWithDistAndLabel>() {
                @Override
                public int compare(PointWithDistAndLabel o1, PointWithDistAndLabel o2) {
                    return o1.getDist().compareTo(o2.getDist());
                }
            });

            //Get Top K
            kNearest = points.subList(0, K);

            //Find Majority
            String predictedLabel = findMajorityLabel(kNearest);

            //Write
            StringBuilder sb = new StringBuilder();
            sb.append(key.toString()).append(" ");
            String kNearestString = kNearest.stream().map(elem -> elem.getTo().getDataLabel().toString()).collect(Collectors.joining(" "));
            sb.append(kNearestString).append(" ");
            sb.append(predictedLabel);
            Text text = new Text(sb.toString());
            context.write(text, NullWritable.get());
        }

        private String findMajorityLabel(List<PointWithDistAndLabel> kNearest) {
            List<Integer> labels = kNearest.stream().map(elem -> Integer.parseInt(elem.getLabel().toString())).collect(Collectors.toList());
            Collections.sort(labels);

            int maxCount = 0;
            int index = -1;
            for (int i = 0; i < labels.size(); i++) {
                int count = 0;
                for (int j = 0; j < labels.size(); j++) {
                    if (labels.get(i).compareTo(labels.get(j)) == 0)
                        count++;
                }

                if (count > maxCount) {
                    maxCount = count;
                    index = i;
                }
            }
            if (maxCount > labels.size() / 2)
                return labels.get(index).toString();

            else
                return "NA";

        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Args length " + args.length + " is not equal to 4!");
            System.err.println("Usage: KNNImpl <in> <out> <test> <K>");
            System.exit(2);
        }

        System.out.println("Args : " + Arrays.toString(args));

        // Create configuration
        Configuration conf = new Configuration();

        Integer k = Integer.parseInt(args[3]);
        conf.setInt("K", k);
        conf.set("distance", "eucl");

        // Create job
        Job job = Job.getInstance(conf, "KNN Classifier");
        job.setJarByClass(KNNImpl.class);
        Path pfs = new Path(args[2]);
        System.out.println("Adding " + pfs.toUri().toString());
        job.addCacheFile(pfs.toUri());

        // Setup MapReduce job
        job.setMapperClass(KnnMapper.class);
        job.setReducerClass(KnnReducer.class);
        job.setNumReduceTasks(1); // Only one reducer in this design

        // Specify key / value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PointWithDistAndLabel.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Input (the data file) and Output (the resulting classification)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Execute job and return status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
