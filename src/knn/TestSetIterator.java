package knn;

import distances.Distance;
import writables.Point;

import java.util.ArrayList;
import java.util.List;

public class TestSetIterator {
    private List<Point> testSet;
    private Distance distance;
    private Point point;

    public TestSetIterator(Distance distance) {
        testSet = new ArrayList<>();
        this.distance = distance;
        point = new Point();
    }

    public void add(String record) {
        point.parse(record);
        testSet.add(point);
    }

    public List<PointWithDistAndLabel> getDistFromAllTestSet(Point point, String label) throws Exception {
        List<PointWithDistAndLabel> result = new ArrayList<>();

        if (testSet.size() <= 0) return null;

        for (Point test : testSet) {
            double dist = distance.getDistance(test, point);
            result.add(new PointWithDistAndLabel(test, point, dist, label));
        }

        return result;
    }
}
