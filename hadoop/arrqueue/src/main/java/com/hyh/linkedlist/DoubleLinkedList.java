package com.hyh.linkedlist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoubleLinkedList<T> {
    private Logger logger = LoggerFactory.getLogger(DoubleLinkedList.class);

    class Node<T> {
        private T value;
        public Node pre;
        public Node next;

        public Node(T value, Node pre, Node next) {
            this.value = value;
            this.pre = pre;
            this.next = next;
        }
    }

    public Node head = null;
    public Node tail = null;

    /**
     * 双向链表添加元素
     *
     * @param ele
     */
    public void add(T ele) {
        Node<T> newNode = new Node<T>(ele, null, null);
        //添加第一个元素
        if (head == null) {
            head = newNode;
        } else {
            //新元素的pre指向tail
            newNode.pre = tail;
            //tail尾部next指向新元素
            tail.next = newNode;
        }
        //新tail
        tail = newNode;
    }

    /**
     * 从双向链表head开始打印元素值
     */
    public void printDoubleLinkedList() {
        if (head != null) {
            Node tmp = head;
            do {
                System.out.println(tmp.value);
                tmp = tmp.next;
            } while (tmp != null && tmp != head);
        }
    }

    /**
     * 判断元素是否在链表中，假定使用Integer类型
     *
     * @param ele
     * @return 存在返回true 不存在返回false
     */
    public boolean contains(T ele) {
        if (head != null) {
            Node tmp = head;
            do {
                if (tmp.value == ele) {
                    return true;
                }
                tmp = tmp.next;
            } while (tmp != null && tmp != head);
        }
        return false;
    }

    /**
     * 删除双向链表的某个元素
     *
     * @param ele
     */
    public void deleteOneEle(T ele) {
        if (!contains(ele)) {
            logger.error("元素不存在无法删除！");
            return;
        }
        Node node = getEle(ele);
        Node pre = node.pre;
        Node next = node.next;
        if (head == tail) {//只有一个元素
            head = null;
            tail = null;
        } else if (head.value == ele) {//删除元素为head
            next.pre = null;
            head = next;
        } else if (tail.value == ele) {//删除元素为tail
            pre.next = null;
            tail = pre;
        } else {
            //删除元素在中间位置
            pre.next = next;
            next.pre = pre;
        }
    }

    /**
     * 根据元素值返回相应Node节点
     *
     * @param ele
     * @return Node
     */
    public Node getEle(T ele) {
        if (!contains(ele)) {
            logger.error("元素不存在！");
            return null;
        }
        Node tmp = head;
        do {
            if (tmp.value == ele) {
                return tmp;
            }
            tmp = tmp.next;
        } while (tmp != null && tmp != head);
        return null;
    }

}
