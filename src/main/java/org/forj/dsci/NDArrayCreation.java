package org.forj.dsci;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import static java.lang.System.out;

public class NDArrayCreation
{

    public static void main(String[] args)
    {
        // columns
        out.println(Nd4j.zeros(3));

        // rows, columns
        out.println(Nd4j.zeros(3,3));

        // n-dimensions
        out.println(Nd4j.zeros(3,3,3));

        INDArray nd2 = Nd4j.create(new float[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, new int[]{2, 6});

    }
}
