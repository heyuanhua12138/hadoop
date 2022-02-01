package com.hyh.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrQueue<T> {
    private Logger logger = LoggerFactory.getLogger(ArrQueue.class);
    //队列的头位置
    private int head = 0;
    //队列下一个元素位置
    private int tail = 0;
    //队列当前大小
    private int count = 0;
    //队列初始值
    private int initSize;
    //底层用数组
    private T[] arr;

    public ArrQueue(int initSize) {
        this.arr = (T[]) new Object[initSize];
        this.initSize = initSize;
    }

    /**
     * 队列添加元素
     *
     * @param ele
     */
    public void input(T ele) {
        if (isFull()) {
            logger.error("队列已满！");
            return;
        }
        arr[tail] = ele;
        count += 1;
        tail += 1;
        if (count == initSize) {
            tail = 0;
        }
    }

    //队列移出元素，返回元素值
    public T output() {
        if (isEmpty()) {
            logger.error("队列为空！");
            return null;
        }
        T t = arr[head];
        count -= 1;
        head += 1;
        if (head == initSize) {
            head = 0;
        }
        return t;
    }

    /**
     * 判断队列是否已满
     *
     * @return false 代表未满,true 代表已满
     */
    public boolean isFull() {
        return count == initSize;
    }

    /**
     * 判断队列是否为空
     *
     * @return false 代表不为空,true 代表为空
     */
    public boolean isEmpty() {
        return count == 0;
    }

    /**
     * 返回队列大小
     *
     * @return count
     */
    public int size() {
        return count;
    }

    /**
     * 打印队列FIFO
     */
    public void printArr(){
        int temp = head;
        for (int i = 0; i < count; i++) {
            System.out.println(arr[temp]);
            temp += 1;
            if(temp == initSize){
                temp = 0;
            }
        }
    }
}
