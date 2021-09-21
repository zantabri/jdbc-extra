package com.deepbluec.vertx.jdbc;

public class DBQueryException extends RuntimeException {

    private int code;
    public DBQueryException(int code, String message) {
        super(message);
        this.code = code;
    }

    @Override
    public String toString() {
        return "DBQueryException{" +
                "code=" + code + "," +
                "message=" + super.getMessage() +
                '}';
    }
}
