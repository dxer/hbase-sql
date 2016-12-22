package org.dxer.hbase.exceptions;

/**
 * Created by linghf on 2016/8/30.
 */

public class NoRowKeyException extends Exception {
    private static final long serialVersionUID = -8980028569652624236L;

    public NoRowKeyException(String string) {
        super(string);
    }

}
