package cn.edu.thssdb.exception;

public class TableDataFileNotExistException extends RuntimeException{
  String name;
  public TableDataFileNotExistException(String name) {
    super();
    this.name = name;
  }

  @Override
  public String getMessage() {
    return String.format("Exception: Data file of table %s doesn't exist!", name);
  }
}
