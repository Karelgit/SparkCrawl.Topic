package com.gengyun.utils;

import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

/**
 * Created by hadoop on 2015/10/30.
 */
public class PropertyHelper {

    private ResourceBundle propBundle;

    public PropertyHelper(String bundle) {
        propBundle = PropertyResourceBundle.getBundle(bundle);
    }

    public String getValue(String key) {
        return this.propBundle.getString(key);
    }

}

