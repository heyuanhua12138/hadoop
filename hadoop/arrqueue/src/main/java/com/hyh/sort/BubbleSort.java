package com.hyh.sort;

public class BubbleSort {
    public static void main(String[] args) {
        int[] intArr = {1, 22, 31, 43, 5};
        for (int j = 0; j < intArr.length - 1; j++) {
            for (int i = 0; i < intArr.length - 1 - j; i++) {
                if (intArr[i] > intArr[i + 1]) {
                    CommonMethod.swap(intArr, i, i + 1);
                }
            }
        }
        for (int i = 0; i < intArr.length; i++) {
            System.out.println(intArr[i]);
        }
    }
}
