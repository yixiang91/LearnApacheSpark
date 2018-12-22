package com.yixiangtay.learning.apache.spark;

public class IntegerWithSquareRoot {

    private int originalNumber;
    private double squareRoot;

    public IntegerWithSquareRoot(int i) {
        this.originalNumber = i;
        this.squareRoot = Math.sqrt(i);
    }

}
