package com.hyh.sort;

public class CommonMethod {

    public static void swap(int[] intArr, int i, int i1) {
        int tmp = intArr[i];
        intArr[i] = intArr[i1];
        intArr[i1] = tmp;
    }
}
