package cn.edu.thssdb.exception;

public class DuplicateKeyException extends RuntimeException{
  @Override
  public String getMessage() {
    return String.format("Exception: Duplicated key in B+ tree!" );
  }
}
