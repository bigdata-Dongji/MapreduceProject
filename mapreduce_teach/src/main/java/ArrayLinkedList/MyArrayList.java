package ArrayLinkedList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MyArrayList {
    public static void main(String[] args) {
        MyList list = new MyList();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        list.add(6);
        list.add(7);
        list.add(8);
        list.add(9);
        list.add(10);
        list.add(11);
        list.add(12);
        list.add(13);
//        Collections.sort();
    }

    //    手写实现的ArrayList，通过数组底层方式实现
    public static class MyList{

        //整个集合最关键的，有一个数组来保存数据
        private Object[] initialization;

        //还有一个很关键的，数组会有长度，保存几个数据，数组长度就为几
        private int size;

        //通过构造方法，初始化一些事情
        //我们的思路是最开始当集合被创建，就给它里面的数组定义一个默认长度
        public MyList(){
            initialization = new Object[10];
        }

        public void add(Object obj){
            //关键问题，数据长度本身不可变，所以当我们把数组长度定为10后
            //如果要超出了，怎么办呢？
            if (size>=initialization.length){
                //当数组容量要不够的时候，来一个新的数组，并且长度翻倍
                Object[] tmp = new Object[initialization.length * 2];
                //数组的复制，我们不能丢掉原先initialization的数据,而是把它复制到tmp里面去
                System.arraycopy(initialization,0,tmp,0,size);
                initialization = tmp;
            }
            initialization[size++]  = obj;
        }

    }
    public static class Node{
        //最核心的变量，用来保存当前节点的数据
        public Object obj;

        //很关键，保存的是前一个节点的数据
        public Node prev;

        //很关键，保存的是后一个节点的数据
        public Node next;

        public void setObj(Object obj){
            this.obj=obj;
        }

        //带参数的构造方法
        public Node(Object obj, Node prev, Node next) {
            this.obj = obj;
            this.prev = prev;
            this.next = next;
        }
        //不带参数的构造方法
        public Node() {
        }
    }
    public static class MyLinkedList{
        private Node first;

        private Node last;

        private int size;

        public void add(Object obj){
            Node node = new Node();
            if (first==null){
                //当第一个节点不存在，集合是空的时候
                node.prev=null;
                node.next=null;
                node.setObj(obj);
                first=node;
                last=node;
            }else {
                //当第一个节点存在后，直接往后面关联就可以了
                node.prev=last;
                node.next=null;
                node.setObj(obj);
                last.next=node;
                last=node;
                //最后，当我们把原先的最后一个节点设置都弄好之后，就可以放心的
                //把这次新加的节点覆盖到last节点了
            }
        }
    }
}
