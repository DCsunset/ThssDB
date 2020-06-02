package cn.edu.thssdb.exception;

import cn.edu.thssdb.schema.Column;

public class ColumnNotExistException extends RuntimeException{
  String name;
  public ColumnNotExistException(String name) {
    super();
    this.name = name;
  }

  @Override
  public String getMessage() {
    return String.format("Exception: Column %s doesn't exist!", name);
  }
}
