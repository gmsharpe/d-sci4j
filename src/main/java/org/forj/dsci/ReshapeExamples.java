package org.forj.dsci;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.linalg.Matrices;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.util.Arrays;
import java.util.List;

import static java.lang.System.out;

public class ReshapeExamples
{

    public static void main(String[] args){

        reshapeWithNd4j();

    }


    public static void reshapeWithNd4j(){

        //https://deeplearning4j.konduit.ai/nd4j/matrix-manipulation

        INDArray nd2 = Nd4j.create(new float[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, new int[]{2, 6});

        out.println(nd2);

        // System.out.println(nd2.reshape(3,4));

        /*
            [1.0 ,4.0 ,7.0 ,10.0]
            [2.0 ,5.0 ,8.0 ,11.0]
            [3.0 ,6.0 ,9.0 ,12.0]
         */


        // row major (c) vs col major (f)
        out.println(nd2.reshape('f', 3,4));

        //out.println(nd2.reshape('c',false,1,2));

        // Exception in thread "main" org.nd4j.linalg.exception.ND4JIllegalStateException:
        // New shape length doesn't match original length: [2] vs [12]. Original shape: [2, 6] New Shape: [1, 2]


        out.println(Nd4j.diag(nd2.reshape(1,12)));

        out.println(Nd4j.toFlattened(nd2));

    }

    public static void reshapeWithSpark(){
        String appName = "app";
        String master = "local[*]";

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder()
                                         .sparkContext(jsc.sc())
                                         .master(master)
                                         .getOrCreate();



        List<Row> data = Arrays.asList(
                RowFactory.create(0, Arrays.asList(1.0, 0.1, -8.0)),
                RowFactory.create(1, Arrays.asList(2.0, 1.0, -4.0)),
                RowFactory.create(2, Arrays.asList(4.0, 10.0, 8.0))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", DataTypes.createArrayType(DataTypes.DoubleType), false, Metadata.empty())
        });

        Dataset<Row> dataFrame = spark.createDataFrame(data, schema);

        dataFrame.show();

        // https://spark.apache.org/docs/latest/mllib-data-types.html#local-matrix
        // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
        Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

        // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
        Matrix sm = Matrices.sparse(3, 2, new int[] {0, 1, 3}, new int[] {0, 2, 1}, new double[] {9, 6, 8});
    }

}
