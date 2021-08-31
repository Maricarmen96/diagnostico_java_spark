package datio;

import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;
import static minsait.ttaa.datio.common.naming.PlayerInput.overall;
import static minsait.ttaa.datio.common.naming.PlayerInput.potential;
import static org.junit.Assert.*;

public class PotentialvsOverallTest {

    static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();


    @Test
    public void e3TestEqual() {
        List<String> data = Arrays.asList(
                "{\""+potential.getName()+"\":\"50\",\""+overall.getName()+"\":\"50\"}"
        );
        Dataset<String> pset = spark.createDataset(data, Encoders.STRING());
        Dataset<Row> rowset = spark.read().json(pset);
        rowset = Transformer.setPotentialVsOverall(rowset);

        assertEquals(Double.doubleToLongBits(1.0),
                Double.doubleToLongBits(rowset.first().getDouble(2)));

    }

    @Test
    public void e3TestNotEqual() {
        List<String> data = Arrays.asList(
                "{\""+potential.getName()+"\":\"59\",\""+overall.getName()+"\":\"78\"}"
        );
        Dataset<String> pset = spark.createDataset(data, Encoders.STRING());
        Dataset<Row> rowset = spark.read().json(pset);
        rowset = Transformer.setPotentialVsOverall(rowset);
        assertNotEquals(Double.doubleToLongBits(1.0), Double.doubleToLongBits(rowset.first().getDouble(2)));
    }
}
