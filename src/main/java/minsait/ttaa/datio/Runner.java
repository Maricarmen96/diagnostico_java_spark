package minsait.ttaa.datio;

import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.sql.SparkSession;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

public class Runner {
    static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .config("spark.debug.maxToStringFields",1000)
            .getOrCreate();

    public static void main(String[] args) {
        Transformer engine = new Transformer(spark);
    }
}
