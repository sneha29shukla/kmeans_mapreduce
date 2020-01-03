package knn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import writables.Point;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PointWithDistAndLabel implements Writable {
    private Point from;
    private Point to;
    private DoubleWritable dist;
    private Text label;

    public PointWithDistAndLabel() {
        this.from = new Point();
        this.to = new Point();
        this.dist = new DoubleWritable();
        this.label = new Text();
    }

    public PointWithDistAndLabel(Point from, Point to, double dist, String label) {
        this();
        this.from = new Point(from.getVector(), (int) from.getNumber(), from.getDataLabel().toString());
        this.to = new Point(to.getVector(), (int) to.getNumber(), to.getDataLabel().toString());
        this.dist.set(dist);
        this.label.set(label);
    }

    public PointWithDistAndLabel(PointWithDistAndLabel point) {
        this();
        this.from = new Point(point.getFrom());
        this.to = new Point(point.getTo());
        this.dist.set(point.getDist());
        this.label.set(point.getLabel().toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        from.write(dataOutput);
        to.write(dataOutput);
        dist.write(dataOutput);
        label.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        from.readFields(dataInput);
        to.readFields(dataInput);
        dist.readFields(dataInput);
        label.readFields(dataInput);
    }

    @Override
    public String toString() {
        return from.toString() + " " + to.toString() + " " + dist.toString() + " " + label.toString();
    }

    public Point getFrom() {
        return from;
    }


    public Point getTo() {
        return to;
    }


    public Double getDist() {
        return dist.get();
    }

    public Text getLabel() {
        return label;
    }

//    @Override
//    public int compareTo(PointWithDistAndLabel o) {
//        return this.getLabel().compareTo(o.getLabel());
//    }
}
