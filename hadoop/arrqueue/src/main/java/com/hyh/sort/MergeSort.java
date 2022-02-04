package com.hyh.sort;

public class MergeSort {
    //归并排序（原地）
    public static void main(String[] args) {
        int[] intArr = {5, 2, 6, 7, 8, 9, 1, 3};
        sort(intArr, 0, intArr.length - 1);
        for (int i = 0; i < intArr.length; i++) {
            System.out.println(intArr[i]);
        }
    }

    private static void sort(int[] intArr, int start, int stop) {
        if (start >= stop) {
            return;
        }
        int mid = (start + stop) / 2;
        sort(intArr, start, mid);
        sort(intArr, mid + 1, stop);
        merge(intArr, start, mid, stop);
    }

    private static void merge(int[] intArr, int start, int mid, int stop) {
        // 截取已经有序的左边数组 [start, mid]
        //val left = arr.slice(start, mid + 1)
        // // 截取已经有序的左边数组 [mid + 1, stop]
        //val right = arr.slice(mid + 1, stop + 1)
        int[] leftArr = new int[mid - start + 1];
        int[] rightArr = new int[stop - mid];
//        for (int i = 0; i < intArr.length; i++) {
//            leftArr[i] = Integer.MAX_VALUE;
//            rightArr[i] = Integer.MAX_VALUE;
//        }
        System.arraycopy(intArr, start, leftArr, 0, mid - start + 1);
        System.arraycopy(intArr, mid+1, rightArr, 0, stop - mid);
        //leftArr[leftArr.length - 1] = Integer.MAX_VALUE;
        //rightArr[rightArr.length - 1] = Integer.MAX_VALUE;
        int leftIndex = 0;
        int rightIndex = 0;
        for (int i = start; i < stop; i++) {
            if (leftIndex == leftArr.length) {
                intArr[i] = rightArr[rightIndex];
                rightIndex++;
            } else if (rightIndex == rightArr.length) {
                intArr[i] = leftArr[leftIndex];
                leftIndex++;
            } else if (leftArr[leftIndex] <= rightArr[rightIndex]) {
                intArr[i] = leftArr[leftIndex];
                leftIndex++;
            } else {
                intArr[i] = rightArr[rightIndex];
                rightIndex++;
            }

//            if (leftArr[leftIndex] <= rightArr[rightIndex]) {
//                intArr[i] = leftArr[leftIndex];
//                leftIndex++;
//            } else {
//                intArr[i] = rightArr[rightIndex];
//                rightIndex++;
//            }
        }
    }

}
