package writables;

import com.sun.istack.NotNull;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class Centroid implements WritableComparable<Centroid> {
    private Text label;
    private Point point;
    private Text points;

    public Centroid() {
        label = new Text();
        point = new Point();
        points = new Text();
    }

    public Centroid(Text label, Point point, Text points) {
        this.label = label;
        this.point = point;
        this.points = points;
    }

    public Centroid(String value) {
        this();
        String[] splits = value.split(",");

        label.set(parseLabel(splits[0]));
        point.parse(splits[1]);
    }

    private static String parseLabel(String split) {
        return split.replace(".", "");
    }

    public String getLabel() {
        return label.toString();
    }

    public Text getText() {
        return label;
    }

    public Point getPoint() {
        return point;
    }

    public Text getPoints() {
        return points;
    }

    @Override
    public int compareTo(@NotNull Centroid centroid) {
        return this.label.compareTo(centroid.getText());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        label.write(dataOutput);
        point.write(dataOutput);
        points.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        label.readFields(dataInput);
        point.readFields(dataInput);
        points.readFields(dataInput);
    }

    @Override
    public String toString() {
        return this.getLabel() + "," + this.getPoint() + "," + this.getPoints();
    }

}
