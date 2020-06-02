package cn.edu.thssdb.exception;

public class DuplicatePrimaryKeyException extends RuntimeException{
  String name;
  public DuplicatePrimaryKeyException(String name) {
    super();
    this.name = name;
  }

  @Override
  public String getMessage() {
    return String.format("Exception: Duplicated Primary key %s!", name);
  }
}
