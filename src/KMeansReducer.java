import distances.Distance;
import enums.KMeansCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import writables.Centroid;
import writables.Point;

import java.util.ArrayList;
import java.util.List;


public class KMeansReducer extends Reducer<Centroid, Point, Text, NullWritable> {
    private Double delta = 0.;
    private Text outKey = new Text();
    private Counter adjusted;
    private Distance distance;
    private Integer dim;

    @Override
    protected void setup(Context context) {
        adjusted = context.getCounter(KMeansCounter.ADJUSTED);
        Configuration conf = context.getConfiguration();
        delta = conf.getDouble("delta", 0.);
        distance = Distance.getDistance(conf.get("distance"));
        dim = conf.getInt("dimension", 0);
    }

    @Override
    protected void reduce(Centroid key, Iterable<Point> values, Context context) {
        List<Point> points = new ArrayList<>();
        values.forEach(points::add);


        Centroid newKey = new Centroid(key.getText(), distance.getExpectation(points), key.getPoints());

        try {
            double similarity = distance.getDistance(key.getPoint(), newKey.getPoint());
            if (similarity > delta) {
                adjusted.increment(1L);
            }

            double count = points.get(0).getNumber();
            String outputKey = newKey.toString() + "," + count;
            outKey.set(outputKey);
            context.write(outKey, NullWritable.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
