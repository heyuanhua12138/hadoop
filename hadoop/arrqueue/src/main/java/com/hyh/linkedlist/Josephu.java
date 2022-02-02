package com.hyh.linkedlist;

public class Josephu {
    //    设编号为1，2，… n的n个人围坐一圈，约定编号为k（1<=k<=n）的人从1开始报数，数到m 的那个人出列，
//    它的下一位又从1开始报数，数到m的那个人又出列，依次类推，直至剩下一个人为止
    public static void main(String[] args) {
        int n = start(10, 1, 3);
        System.out.println(n);
    }

    private static int start(int n, int start, int m) {
        CircleDoubleLinkedList<Integer> list = new CircleDoubleLinkedList<Integer>();
        for (int i = 1; i <= n; i++) {
            list.add(i);
        }
        DoubleLinkedList.Node startNode = list.getEle(start).pre;
        while (list.head != list.tail) {
            for (int j = 1; j <= m; j++) {
                startNode = startNode.next;
            }
            list.deleteOneEle(startNode.value);
            //System.out.println(">"+startNode.value);
            startNode = startNode.pre;
        }
        return (Integer) startNode.value;
    }

}
