package cn.edu.thssdb.exception;

public class TableNotExistException extends RuntimeException{
  String name;
  public TableNotExistException(String name) {
    super();
    this.name = name;
  }

  @Override
  public String getMessage() {
    return String.format("Exception: Table %s doesn't exist!", name);
  }
}
