package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.utils.Global;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;
  private static CommandLine commandLine;

  public static void main(String[] args) {
    long sessionId = -1;
    commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }
    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      transport = new TSocket(host, port);
      transport.open();
      protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      boolean open = true;
      while (true) {
        print(Global.CLI_PREFIX);
        String msg = SCANNER.nextLine();

        long startTime = System.currentTimeMillis();
        switch (msg.trim().split(" ")[0]) {
          case Global.SHOW_TIME:
            getTime();
            break;
          case Global.QUIT:
            open = false;
            break;
          case Global.CONNECT:
            String[] params = msg.split(" ");
            if (params.length != 3) {
              println("Connect <username> <password>");
              break;
            }
            if (sessionId != -1) {
              println("Please disconnect first!");
              break;
            }
            sessionId = connect(params[1], params[2]);
            println(String.format("sessionId=%d", sessionId));
            break;
          case Global.DISCONNECT:
            if (sessionId == -1) {
              println("Cannot disconnect before connect!");
              break;
            }
            disconnect(sessionId);
            sessionId = -1;
            break;
          default:
            if (sessionId == -1) {
              println("Cannot execute SQL query before connect!");
              break;
            }
            // SQL statement
            executeSQL(sessionId, msg);
            break;
        }
        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");
        if (!open) {
          break;
        }
      }
      transport.close();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    } catch (TException e) {

    }
  }

  static void executeSQL(long id, String query) {
    ExecuteStatementReq req = new ExecuteStatementReq(id, query);
    try {
      ExecuteMultiStatementResp resp = client.executeMultiStatement(req);
      for (ExecuteStatementResp r : resp.getResults()) {
          for (String c : r.getColumnsList())
            System.out.print(c + "\t");
          System.out.println();
          for (List<String> row : r.getRowList()) {
            for (String value : row)
              System.out.print(value + "\t");
            System.out.println();
          }
      }
    }
    catch (Exception e) {
      logger.error(e.getMessage());
    }
}

  private static void disconnect(long id) {
    DisconnectReq req = new DisconnectReq();
    req.sessionId = id;
    try {
      client.disconnect(req);
    } catch (TException e) {

      logger.error(e.getMessage());
    }
  }

  private static long connect(String username, String password) {
    ConnectReq req = new ConnectReq();
    req.password = password;
    req.username = username;
    try {
      ConnectResp resp = client.connect(req);
      return resp.sessionId;
    } catch (TException e) {
      logger.error(e.getMessage());
      return -1;
    }
  }

  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_ARGS).argName(HELP_NAME).desc("Display help information(optional)")
        .hasArg(false).required(false).build());
    options.addOption(Option.builder(HOST_ARGS).argName(HOST_NAME).desc("Host (optional, default 127.0.0.1)")
        .hasArg(false).required(false).build());
    options.addOption(Option.builder(PORT_ARGS).argName(PORT_NAME).desc("Port (optional, default 6667)").hasArg(false)
        .required(false).build());
    return options;
  }

  static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  static void showHelp() {
    // TODO
    println("DO IT YOURSELF");
  }

  static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }
}
