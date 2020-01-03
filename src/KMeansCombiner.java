import distances.Distance;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import writables.Centroid;
import writables.Point;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class KMeansCombiner extends Reducer<Centroid, Point, Centroid, Point> {

    @Override
    protected void reduce(Centroid key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        List<Point> pointList = new ArrayList<>();

        for (Point value : values) {
            Point p = new Point(value);
            pointList.add(p);
        }
        Point result = Distance.sumPoints(pointList);
        Text points = new Text(pointList.stream().map(p -> p.getDataLabel().toString()).collect(Collectors.joining(" ")));
        Centroid newKey = new Centroid(key.getText(), key.getPoint(), points);

        if (result != null) {
            context.write(newKey, result);
        }
    }
}
