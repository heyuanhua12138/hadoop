import com.hyh.queue.ArrQueue;
import org.junit.Test;

public class TestArrQueue {
    @Test
    public void test(){
        ArrQueue<Integer> integerArrQueue = new ArrQueue<Integer>(5);
        integerArrQueue.input(1);
        integerArrQueue.input(2);
        integerArrQueue.input(3);
        integerArrQueue.input(4);
        integerArrQueue.input(5);
        integerArrQueue.printArr();
        System.out.println(integerArrQueue.output());
        integerArrQueue.input(6);
        System.out.println("队列大小==="+integerArrQueue.size());
//        System.out.println(integerArrQueue.output());
//        System.out.println(integerArrQueue.output());
//        System.out.println(integerArrQueue.output());
//        System.out.println(integerArrQueue.output());
//        System.out.println(integerArrQueue.output());
        integerArrQueue.printArr();
    }
}
