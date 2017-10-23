package org.kin.kafka.multithread.distributed;

import org.kin.kafka.multithread.distributed.node.Node;

/**
 * Created by huangjianqin on 2017/10/23.
 */
public class NodeRunner {
    public static void main(String[] args) {
        Node node = null;
        try{
            node = new Node();
            node.init();
            node.start();
        }finally {
            if(node != null){
                node.close();
            }
        }
    }
}
