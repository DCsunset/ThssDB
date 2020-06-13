package cn.edu.thssdb.server;

import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.storage.DbCache;
import cn.edu.thssdb.utils.Global;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThssDB {

  private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

  private static IServiceHandler handler;
  private static IService.Processor processor;
  private static TServerSocket transport;
  private static TServer server;

  private Manager manager;

  public static ThssDB getInstance() {
    return ThssDBHolder.INSTANCE;
  }

  public static void main(String[] args) {
    ThssDB server = ThssDB.getInstance();
    server.start();
  }

  private void start() {
    // Get users
    BufferedReader reader;
    HashMap<String, String> users = new HashMap<>();
    try {
      String currentDir = new File("").getAbsolutePath();
      System.out.println("currentDir=" + currentDir);
      // reader = new BufferedReader(new FileReader(currentDir.concat("users")));
      reader = new BufferedReader(new FileReader("users"));
      String line = reader.readLine();
      while (line != null) {
        String[] strs = line.split("\t");
        for (String str : strs) {
          System.out.println(str);
        }
        users.put(strs[0], strs[1]);
        line = reader.readLine();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    handler = new IServiceHandler(users);
    processor = new IService.Processor(handler);
    Runnable setup = () -> setUp(processor);
    new Thread(setup).start();
  }

  private static void setUp(IService.Processor processor) {
    try {
      transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
      server = new TThreadPoolServer(new TThreadPoolServer.Args(transport).processor(processor));
      logger.info("Starting ThssDB ...");
      server.serve();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static class ThssDBHolder {
    private static final ThssDB INSTANCE = new ThssDB();

    private ThssDBHolder() {

    }
  }
}
