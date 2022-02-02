package com.hyh.linkedlist;

import java.util.Iterator;
import java.util.LinkedList;

public class JosephuJDK {
    //    设编号为1，2，… n的n个人围坐一圈，约定编号为k（1<=k<=n）的人从1开始报数，数到m 的那个人出列，
//    它的下一位又从1开始报数，数到m的那个人又出列，依次类推，直至剩下一个人为止
    public static void main(String[] args) {
        int n = start(10, 1, 3);
        System.out.println(n);
    }

    private static int start(int n, int start, int m) {
        LinkedList<Integer> list = new LinkedList<Integer>();
        for (int i = 1; i <= n; i++) {
            list.add(i);
        }
        Iterator<Integer> iterator = list.iterator();
        int count = 0;
        while (list.size()>1){
            Integer next=0;
            if(iterator.hasNext()){
                next = iterator.next();
                count++;
            }else{
                iterator = list.iterator();
            }

            if(count==m){
                count=0;
                //System.out.println("移除"+next);
                iterator.remove();
            }
        }
        return list.getFirst();
    }

}
