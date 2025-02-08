package cz.it4i.fiji.datastore.bdv_server;

public class DataReturn {
    public enum ReturnType {
        XML,
        JSON,
        BASE64
    }
    private final ReturnType returnType;
    private final String data;

    public DataReturn(ReturnType returnType, String data) {
        this.returnType = returnType;
        this.data = data;
    }

    public ReturnType getReturnType() {
        return this.returnType;
    }

    public String getData() {
        return this.data;
    }
}
