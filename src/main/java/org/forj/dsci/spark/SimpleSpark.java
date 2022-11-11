package org.forj.dsci.spark;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.spark.*;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.linalg.QRDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.random.RandomRDDs;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.ml.linalg.*;
import org.apache.spark.sql.types.*;
import org.nd4j.linalg.factory.Nd4j;
import scala.Tuple2;
import scala.Unit;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.convert.AsScalaConverters;
import scala.concurrent.JavaConversions;
import smile.math.random.RandomNumberGenerator;
import smile.stat.distribution.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.System.out;

public class SimpleSpark
{
    public static void main(String[] args)
    {
        String appName = "app";
        String master = "local[*]";

        SparkConf conf = new SparkConf().setAppName(appName).set("spark.ui.showConsoleProgress", "false").setMaster(
                master);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SparkSession sparkSession = SparkSession.builder()
                                                .sparkContext(jsc.sc())
                                                .master(master)
                                                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("WARN");

        Function<Integer, double[]> dubFunc = x -> {
            double[] ia = new double[x*x];
            Arrays.fill(ia, x);
            return ia;
        };

        coordinateRowMatrices();
        //indexedRowMatrices();

        //statisticalDistributions();
    }

    public static void rdds() {
        JavaRDD<org.apache.spark.mllib.linalg.Vector> normallyDistributedVectorRDD =
                RandomRDDs.normalJavaVectorRDD(JavaSparkContext.fromSparkContext(SparkSession.active().sparkContext()),
                                               10, 10);
        out.println(normallyDistributedVectorRDD.take(2));
        double[] dubs =
                normallyDistributedVectorRDD.collect().stream().map(Vector::toArray).reduce(ArrayUtils::addAll).get();
        System.out.println(Arrays.toString(dubs));

        RowMatrix mat = new RowMatrix(normallyDistributedVectorRDD.rdd());
        

        JavaDoubleRDD normallyDistributed =
                RandomRDDs.normalJavaRDD(JavaSparkContext.fromSparkContext(SparkSession.active().sparkContext()), 100, 100);
        out.println(normallyDistributed.top(100));
        out.println(normallyDistributed.variance());
        out.println(normallyDistributed.mean());


        //Matrix mat2 = Matrices.dense(10, 10, ArrayUtils.toPrimitive(normallyDistributed.rdd().collect()));
    }

    public static void blockMatrices()
    {
        // https://spark.apache.org/docs/latest/mllib-data-types.html#blockmatrix


        // a JavaRDD of (i, j, v) Matrix Entries
        List<MatrixEntry> matrixEntries =
                Arrays.asList(new MatrixEntry(0,1,1.2),
                              new MatrixEntry(0,2,1.5),
                              new MatrixEntry(0,3,2.5));

        // a JavaRDD of matrix entries
        JavaRDD<MatrixEntry> entries =
                new JavaSparkContext(SparkSession.active()
                                                 .sparkContext()).parallelize(matrixEntries);

        // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
        CoordinateMatrix coordMat = new CoordinateMatrix(entries.rdd());

        // Transform the CoordinateMatrix to a BlockMatrix
        BlockMatrix matA = coordMat.toBlockMatrix().cache();

        // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
        // Nothing happens if it is valid.
        matA.validate();
    }

    public static void rowMatrices()
    {

        // a JavaRDD of local vectors
        JavaRDD<Vector> rows = RandomRDDs.normalJavaVectorRDD(
                JavaSparkContext.fromSparkContext(SparkSession.active().sparkContext()),
                3, 3);

        // Create a RowMatrix from an JavaRDD<Vector>.
        RowMatrix mat = new RowMatrix(rows.rdd());

        // https://en.wikipedia.org/wiki/Gram_matrix
        // https://spark.apache.org/docs/2.2.0/api/java/org/apache/spark/mllib/linalg/distributed/RowMatrix.html#computeGramianMatrix--
        out.println(mat.computeGramianMatrix());


        // QR decomposition - https://en.wikipedia.org/wiki/QR_decomposition
        // https://spark.apache.org/docs/2.2.0/api/java/org/apache/spark/mllib/linalg/distributed/RowMatrix.html#tallSkinnyQR-boolean-
        QRDecomposition<RowMatrix, org.apache.spark.mllib.linalg.Matrix> result = mat.tallSkinnyQR(true);
        out.println(result.R());

    }

    public static void indexedRowMatrices()
    {
        // a JavaRDD of local vectors
        JavaRDD<Vector> rows = RandomRDDs.normalJavaVectorRDD(
                JavaSparkContext.fromSparkContext(SparkSession.active().sparkContext()),
                3, 3);


        JavaRDD<IndexedRow> indexedRowsFromRowMatrix =
               rows.zipWithIndex().map((Tuple2<Vector, Long> t) -> new IndexedRow(t._2(), t._1()));

        // Create an IndexedRowMatrix from a JavaRDD<IndexedRow>.
        IndexedRowMatrix mat = new IndexedRowMatrix(indexedRowsFromRowMatrix.rdd());

        mat.rows().toLocalIterator().foreach(r -> { out.println(r); return r;});

        // Drop its row indices.
        RowMatrix rowMat = mat.toRowMatrix();
    }

    public static void coordinateRowMatrices()
    {

        List<MatrixEntry> matrixEntries =
                Arrays.asList(new MatrixEntry(0,1,1.2),
                              new MatrixEntry(0,2,1.5),
                              new MatrixEntry(0,3,2.5));

        // a JavaRDD of matrix entries
        JavaRDD<MatrixEntry> entries =
                new JavaSparkContext(SparkSession.active()
                            .sparkContext()).parallelize(matrixEntries);

        // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
        CoordinateMatrix mat = new CoordinateMatrix(entries.rdd());

        mat.entries().toLocalIterator().foreach(r -> { out.println(r); return r;});

        // Convert it to an IndexRowMatrix whose rows are sparse vectors.
        // IndexedRowMatrix indexedRowMatrix = mat.toIndexedRowMatrix();

    }

    public static void matrixOps()
    {
        // https://spark.apache.org/docs/latest/mllib-data-types.html#local-matrix
        // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
        Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});
        System.out.println(dm);

        Matrix transposedDm = dm.transpose();
        System.out.println(transposedDm);

        Matrix dm2 = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

        // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
        Matrix sm = Matrices.sparse(3, 2, new int[] {0, 1, 3}, new int[] {0, 2, 1}, new double[] {9, 6, 8});

        Matrix result = dm.multiply((DenseMatrix)transposedDm);
        System.out.println(result.toString());

        breeze.linalg.Matrix<Object> breezeMtrx = dm.asBreeze();

        Vectors.zeros(5);
        Matrices.randn(5,5,new Random());

        DenseVector root = new DenseVector(new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});
        DenseVector result2 = new DenseVector(new double[] {0,0,0,0,0,0});

        BLAS.axpy(3.0, root , result2);

        System.out.println(result2);

        /*

        static void	axpy(double a, Vector x, Vector y)
        y += a * x

        static void	copy(Vector x, Vector y)
        y = x

        static double	dot(Vector x, Vector y)
        dot(x, y)

        static void	gemm(double alpha, Matrix A, DenseMatrix B, double beta, DenseMatrix C)
        C := alpha * A * B + beta * C

        static void	gemv(double alpha, Matrix A, Vector x, double beta, DenseVector y)
        y := alpha * A * x + beta * y

        static void	scal(double a, Vector x)
        x = a * x

        static void	spr(double alpha, Vector v, DenseVector U)
        Adds alpha * v * v.t to a matrix in-place.

        static void	spr(double alpha, Vector v, double[] U)
        Adds alpha * v * v.t to a matrix in-place.

        static void	syr(double alpha, Vector x, DenseMatrix A)
        A := alpha * x * x^T^ + A
         */





    }

    public static void statisticalDistributions()
    {
        /***** XYZ DISTRIBUTION *******/
        // https://introcs.cs.princeton.edu/java/stdlib/

        // random Bernoulli with p = .4 and shape = 2L,2L
        //out.println(Nd4j.randomBernoulli(.4, 2L,2L));
     /*   out.println(Matrices.ones(2,2)
                            .multiply(Vectors.dense(new BinomialDistribution(4, .7).sample(4))));*/

        out.println(Vectors.dense(new GaussianDistribution(0,1).rand(100)));
        out.println(Vectors.dense(new GaussianDistribution(52,13).rand(100)));


        BiFunction<Integer, Distribution, double[]> statDistArrayOfDoubles =
                (size, distribution) -> distribution.rand(size);


        Pair<Integer, Integer> dimensions = Pair.of(4,6);
        //double[] dubs = dubFunc.apply(size);

        BiFunction<Pair<Integer,Integer>, Distribution, Matrix> statDistMatrix = (dim, distribution) ->
                Matrices.dense(dim.getLeft(), dim.getRight(), statDistArrayOfDoubles.apply(dim.getLeft() * dim.getRight(),
                                                                                           distribution));

        out.println(Matrices.dense(dimensions.getLeft(), dimensions.getRight(),
                                   statDistArrayOfDoubles.apply(dimensions.getLeft() * dimensions.getRight(),
                                                                new BernoulliDistribution(.8))));

        out.println(statDistMatrix.apply(dimensions, new GaussianDistribution(60, 12)));

        out.println(statDistMatrix.apply(dimensions, new ExponentialDistribution(.4)));


        // uniform
        out.println(Matrices.rand(2, 2, new Random(1)));

        // gaussian
        out.println(Matrices.randn(2, 2, new Random(1)));

        // random Bernoulli with p = .4 and shape = 2L,2L,2L (add more long args for more dimensions)
        out.println(Nd4j.randomBernoulli(.4, 2L,2L,2L));

        // random Binomial with 3 trials, p = .4 and 2 x 2 shape
        out.println(Nd4j.randomBinomial(3,.4, 2L,2L));

        // random exponentional with lambda = .2 and 2 x 2 shape
        out.println(Nd4j.randomExponential(.2, 2L,2L));

        // create from array where array can be byte, double, float, int, long, boolean, etc.
        out.println(Nd4j.createFromArray(new boolean[] {true, false, true, true}));

        // same but with 2d shape
        out.println(Nd4j.createFromArray(new boolean[][] {{true, false}, {true, true}}));
    }

    public static void creation()
    {
        /***** ZEROS ********/

        // columns
        out.println(Vectors.zeros(3));
        out.println(Matrices.zeros(1,3));
        out.println(Vectors.dense(new double[3]));

        // rows, columns
        out.println(Matrices.zeros(3,3));
       /* out.println(Matrices.fromVectors(JavaConverters.asScalaBuffer(Arrays.asList(Vectors.zeros(3),
                                                                                    Vectors.zeros(3),
                                                                                    Vectors.zeros(3))).toList()));*/


        // n-dimensions - todo
        // challenging in Spark - See https://databricks.com/session/bolt-building-a-distributed-ndarray



        /***** ONES ********/

        // vectors
        double[] arr = new double[3];
        Arrays.fill(arr, 1.0);
        out.println(Vectors.dense(arr));

        // matrices
        out.println(Matrices.ones(3,3));

        // n-dimensions
        // ND4J -> out.println(Matrices.ones(3,3,3));
        // n-dimensions - todo
        // challenging in Spark - See https://databricks.com/session/bolt-building-a-distributed-ndarray


        /***** CONSTANT ********/

        // vectors
        double[] arr2 = new double[3];
        Arrays.fill(arr2, 1.0);
        out.println(Vectors.dense(arr2));

        // n x n matrix
        double[] array = new double[4];
        Arrays.fill(array, 7.0);
        Matrices.dense(2, 2, array);

        Matrices.dense(2, 2, new double[]{7.0, 7.0, 7.0, 7.0});

        // Spark matrices require 'doubles'
        // n x n with double constant
        //out.println(Nd4j.valueArrayOf(new int[]{2,2},7.));

        // n x n with float constant
        // out.println(Nd4j.valueArrayOf(new int[]{2,2,2},7.f));


        /***** IDENTITY MATRIX ********/

        // n x n identity matrix (e.g. n = 4 below)
        //out.println(Nd4j.eye(4L));
        out.println(Matrices.eye(4));



    }

}
