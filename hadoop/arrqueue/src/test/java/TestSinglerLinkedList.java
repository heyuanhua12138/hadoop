import com.hyh.linkedlist.DoubleLinkedList;
import com.hyh.linkedlist.SinglerLinkedList;
import org.junit.Test;

public class TestSinglerLinkedList {
    @Test
    public void test(){
        SinglerLinkedList<Integer> integerSinglerLinkedList = new SinglerLinkedList<Integer>();
        integerSinglerLinkedList.add(1);
        integerSinglerLinkedList.add(2);
        integerSinglerLinkedList.add(3);
        integerSinglerLinkedList.printSinglerLinkedList();
    }

    @Test
    public void test2(){
        DoubleLinkedList<Integer> doubleLinkedList = new DoubleLinkedList<Integer>();
        doubleLinkedList.add(1);
        //doubleLinkedList.add(2);
        //doubleLinkedList.add(3);
        doubleLinkedList.printDoubleLinkedList();

        doubleLinkedList.deleteOneEle(1);
        doubleLinkedList.printDoubleLinkedList();
    }
}
