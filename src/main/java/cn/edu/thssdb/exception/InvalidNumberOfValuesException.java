package cn.edu.thssdb.exception;

public class InvalidNumberOfValuesException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: Invalid number of values";
    }
}
