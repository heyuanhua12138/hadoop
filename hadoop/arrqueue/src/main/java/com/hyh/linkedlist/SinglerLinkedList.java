package com.hyh.linkedlist;

public class SinglerLinkedList<T> {
    class Node<T> {
        private T value;
        private Node next;

        public Node(T value, Node next) {
            this.value = value;
            this.next = next;
        }
    }

    //头节点
    private Node head = null;
    //尾节点
    private Node tail = null;

    /**
     * 单向链表添加元素
     *
     * @param ele 元素
     */
    public void add(T ele) {
        Node<T> newNode = new Node<T>(ele, null);
        if (head == null) {
            //首次添加元素
            head = newNode;
        } else {
            //从tail尾部添加元素
            tail.next = newNode;
        }
        tail = newNode;
    }

    /**
     * 从head开始打印所有元素值
     */
    public void printSinglerLinkedList() {
        if (head != null) {
            Node tmp = head;
            do {
                System.out.println(tmp.value);
                tmp = tmp.next;
            } while (tmp != null);
        }
    }



}
