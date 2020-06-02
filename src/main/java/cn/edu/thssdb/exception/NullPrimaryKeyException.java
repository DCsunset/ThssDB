package cn.edu.thssdb.exception;

public class NullPrimaryKeyException extends RuntimeException{
  String name;
  public NullPrimaryKeyException(String name) {
    super();
    this.name = name;
  }

  @Override
  public String getMessage() {
    return String.format("Exception: Primary key %s can't be null!", name);
  }
}
