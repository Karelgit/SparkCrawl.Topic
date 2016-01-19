package com.gengyun.huanghai;

/**
 * Created by Administrator on 2015/11/17.
 */
public class ArrayStack implements StackADT {

    private Object[] contents;
    private int top;//top标记下一个入栈的位置，同时也表示栈的容量大小，跟链式实现的count比较一下！！！
    private static int SIZE = 10;
    public ArrayStack()
    {
        contents = new Object[SIZE];
        top = 0;
    }
    public void expand(){//借助于申请一个辅助空间,每次扩展容量一倍
        Object[] larger = new Object[size()*2];
        for(int index = 0;index < top;index++)
            larger[index] =  contents[index];
        contents = larger;
    }
    public int size() {
        return top;
    }
    public boolean isEmpty() {
        return (size() == 0);
    }
    public void push(Object element) {
        //if(isEmpty())
        //expand();
        if(top == contents.length)
            expand();
        contents[top] = element;
        top++;
    }
    public Object pop() {
        if(isEmpty())
        {
            System.out.println("stack is empty!");
            System.exit(1);
        }
        Object result = contents[top-1];
        contents[top-1] = null;//出栈
        top--;
        return result;
        /*书上这样写简便一点：：：
         * top--;
         * Object result = contents[top];
         * contents[top] = null;*/
    }
    public Object peek() {
        Object result;
        if(isEmpty())
            result = null;
        else
            result = contents[top-1];
        return result;
    }
    public static void main(String[] args) {
        ArrayStack stack = new ArrayStack();
        System.out.println("将0到24依次压栈，然后连续10次出栈");
        for(int i = 0;i < 25;i++)
            stack.push(i);
        for(int i = 0;i < 10;i++)
            stack.pop();
        System.out.println("栈的大小为： " + stack.size());
        System.out.println("栈为空吗？： " + stack.isEmpty());
        System.out.println("栈顶元素为： " + stack.peek());
    }
}
