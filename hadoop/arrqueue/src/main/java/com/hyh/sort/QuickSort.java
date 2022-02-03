package com.hyh.sort;

public class QuickSort {
    //快排，原地排序
    public static void main(String[] args) {
        //int[] intArr = {1, 22, 31, 43, 5};
        int[] intArr = {1};
        //int[] intArr = {5, 2, 6, 7, 8, 9, 1};
        quickSort(intArr, 0, intArr.length - 1);
        for (int i = 0; i < intArr.length; i++) {
            System.out.println(intArr[i]);
        }
    }

    private static int partition(int[] intArr, int left, int right) {
        int i = left;
        int j = right;
        //i从左往右找，找到大于位置arr[left]的数
        //j从右往左找，找到小于位置arr[left]的数
        //i，j交换位置
        while (i < j) {
            while (i <= right && intArr[i] <= intArr[left]) {
                i++;
            }
            while (j >= i && intArr[j] > intArr[left]) {
                j--;
            }
            if (i < j) {
                CommonMethod.swap(intArr, i, j);
            }
        }
        //交换left和j的位置
        CommonMethod.swap(intArr, left, j);
        //最后返回j的位置即为中间位置
        return j;
    }

    private static void quickSort(int[] intArr, int left, int right) {
        if (left >= right) {//左右指针重合则跳出递归
            return;
        }
        int mid = partition(intArr, left, right);
        quickSort(intArr, left, mid - 1);
        quickSort(intArr, mid + 1, right);
    }
}
