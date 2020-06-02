package cn.edu.thssdb.exception;

public class DatabaseNotExistException extends RuntimeException{
  String name;
  public DatabaseNotExistException(String name) {
    super();
    this.name = name;
  }

  @Override
  public String getMessage() {
    return String.format("Exception: Database %s doesn't exist!", name);
  }
}
