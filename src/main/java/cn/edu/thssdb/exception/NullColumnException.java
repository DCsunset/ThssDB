package cn.edu.thssdb.exception;

public class NullColumnException extends RuntimeException{
  String name;
  public NullColumnException(String name) {
    super();
    this.name = name;
  }

  @Override
  public String getMessage() {
    return String.format("Exception: Column %s can't be null!", name);
  }
}
