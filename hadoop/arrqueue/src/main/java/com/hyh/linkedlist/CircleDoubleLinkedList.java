package com.hyh.linkedlist;

public class CircleDoubleLinkedList extends DoubleLinkedList {
    @Override
    public void add(Object ele) {
        super.add(ele);
        if(head!=null){
            head.pre = tail;
        }
        if(tail!=null){
            tail.next = head;
        }
    }

    @Override
    public void deleteOneEle(Object ele) {
        super.deleteOneEle(ele);
        if(head!=null){
            head.pre = tail;
        }
        if(tail!=null){
            tail.next = head;
        }
    }

}
