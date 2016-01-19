package com.gengyun.urlfilter;

/**
 * Created by hadoop on 2015/11/9.
 */

import java.io.IOException;
import java.io.Serializable;

import com.gengyun.metainfo.BaseURL;
import com.gengyun.utils.LogManager;
import org.apache.spark.api.java.function.Function;


public class RDDPostExpansionFilterEnforcer extends FilterEnforcer implements Serializable {
    private static transient LogManager logger = new LogManager(RDDPostExpansionFilterEnforcer.class);
    private static final long serialVersionUID = 2710224297826136116L;

    public Function<BaseURL, Boolean> filter(){
        RDDPostExpansionFilterEnforcer enf = this;
        Function<BaseURL, Boolean> result = new Function<BaseURL, Boolean>() {
            public Boolean call(BaseURL base) {
                try {
                    boolean result = applyFilters(base);
                    return result;
                } catch (IOException e) {
                    throw  new RuntimeException(e);
                }
            }
        };
        return result;
    }
}
