package jongiddy.barefoot;

import com.bmwcarit.barefoot.road.BaseRoad;
import com.bmwcarit.barefoot.road.RoadReader;
import com.bmwcarit.barefoot.util.SourceException;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.SpatialReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.HashSet;


/**
 * HDFS Map Reader
 *
 */
public class HdfsMapReader implements RoadReader
{
    private URI uri = null;
    private ObjectInput reader = null;
    private HashSet<Short> exclusion = null;
    private Polygon polygon = null;

    HdfsMapReader(URI uri) {
        this.uri = uri;
    }

    public boolean isOpen() {
        return reader != null;
    }

    public void open() throws SourceException {
        open(null, null);
    }

    public void open(Polygon polygon, HashSet<Short> exclusion) throws SourceException {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(uri, conf);
            InputStream in = fs.open(new Path(uri));
            reader = new ObjectInputStream(in);
            this.exclusion = exclusion;
            this.polygon = polygon;
        } catch (Exception e) {
            throw new SourceException(e.getMessage());
        }
    }

    public void close() throws SourceException {
        try {
            if (isOpen()) {
                reader.close();
                reader = null;
            }
        } catch (IOException e) {
            throw new SourceException(e.getMessage());
        }
    }

    public BaseRoad next() throws SourceException {
        SpatialReference gcsWGS84 = SpatialReference.create(4326);
        if (!isOpen()) {
            throw new SourceException("File closed");
        }

        try {
            BaseRoad road;
            do {
                road = (BaseRoad)reader.readObject();
            } while (road != null && (
                    exclusion != null && exclusion.contains(road.type())
                    ||
                    polygon != null && !GeometryEngine.contains(polygon, road.geometry(), gcsWGS84)
                            && !GeometryEngine.overlaps(polygon, road.geometry(), gcsWGS84)
                    )
              );
            return road;
        } catch (ClassNotFoundException e) {
            throw new SourceException("Object read is not a road.");
        } catch (IOException e) {
            throw new SourceException(e.getMessage());
        }
    }
}
