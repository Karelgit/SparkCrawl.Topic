package com.gengyun.huanghai;

/**
 * Created by Administrator on 2015/11/17.
 */
public interface StackADT {
    public void push(Object element);//压栈
    public Object pop();//出栈
    public boolean isEmpty();
    public int size();
    public Object peek();//返回栈顶对象的一个引用
    public String toString();
}
