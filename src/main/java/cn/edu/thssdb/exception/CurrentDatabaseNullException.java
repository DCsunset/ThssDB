package cn.edu.thssdb.exception;

public class CurrentDatabaseNullException extends RuntimeException{
  public CurrentDatabaseNullException() {
    super();
  }

  @Override
  public String getMessage() {
    return "Exception: Current database is null";
  }
}
