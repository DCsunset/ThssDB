package cn.edu.thssdb.utils;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;

  public static String CLI_PREFIX = "ThssDB> ";
  public static final String SHOW_TIME = "show time;";
  public static final String CONNECT = "connect";
  public static final String DISCONNECT = "disconnect";
  public static final String QUIT = "quit;";

  public static final String S_URL_INTERNAL = "jdbc:default:connection";

  // File
  public static final int INIT_FILE_SIZE = 16 * 1024;
  // Page
  public static final int BITMAP_SIZE = 64; // in bit
  public static final int PAGE_SIZE = 4 * 1024;
  // Cache
  public static final int CACHE_SIZE = 1024;

  public static enum OpType {
    EQ, LT, GT, LE, GE, NE
  }

  public static int checkInterval = 3000;
}
