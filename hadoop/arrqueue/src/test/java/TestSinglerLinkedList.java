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
}
