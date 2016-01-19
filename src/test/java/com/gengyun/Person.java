package com.gengyun;

import java.io.Serializable;

/**
 * Created by root on 16-1-4.
 */
public class Person implements Serializable {
    private int id;
    private String name;

    public Person(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
