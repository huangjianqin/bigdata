package org.kin.framework.hotswap;

/**
 * Created by huangjianqin on 2018/10/31.
 */
public class HotSwapMain {
    public static void main(String[] args) {
        Test test = new Test();
        FileMonitor monitor = FileMonitor.instance();
        int i = 0;
        while (true) {
            try {
                Thread.sleep(10000);
                System.out.println(test.message());
                i++;
                if (i % 10 == 0) {
                    test = new Test();
                    System.out.println("new obj");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
