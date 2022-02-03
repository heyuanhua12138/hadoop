package com.hyh.sort;

public class InsertSort {
    public static void main(String[] args) {
        int[] intArr = {1, 22, 31, 43, 5};
        for (int i = 0; i < intArr.length - 1; i++) {//len-1个元素需要排序
            for (int j = i + 1; j > 0; j--) {//从i+1位置开始向前比较
                if (intArr[j] < intArr[j - 1]) {//后面的位置比前面的小则交换位置
                    CommonMethod.swap(intArr, j, j - 1);
                }else{
                    break;
                }
            }
        }
        for (int i = 0; i < intArr.length; i++) {
            System.out.println(intArr[i]);
        }
    }
}
