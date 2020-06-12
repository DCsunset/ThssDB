package cn.edu.thssdb.service;

import cn.edu.thssdb.executor.SQLExecutor;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.storage.DataFile;
import cn.edu.thssdb.storage.DbCache;
import cn.edu.thssdb.storage.MetaFile;
import cn.edu.thssdb.storage.Metadata;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;
import cn.edu.thssdb.storage.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.UUID;

public class IServiceHandler implements IService.Iface {
  private SQLExecutor executor;

  private HashSet<Long> ids = new HashSet<>();
  private HashMap<String, String> users = new HashMap<>();

  public IServiceHandler(HashMap<String, String> users) {
    executor = new SQLExecutor();
    this.users = users;
  }

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    String username = req.username;
    String password = req.password;
    ConnectResp resp = new ConnectResp();
    Status status = new Status();
    if (users.get(username) != null && users.get(username).equals(password)) {
      resp.sessionId = System.nanoTime();
      ids.add(resp.sessionId);
      status.code = Global.SUCCESS_CODE;
      status.msg = "Connect success!";
    } else {
      resp.sessionId = -1;
      status.code = Global.FAILURE_CODE;
      status.msg = "Invalid user";
    }
    resp.status = status;
    return resp;
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    long id = req.sessionId;
    Status result = new Status();
    if (ids.contains(id)) {
      ids.remove(id);
      result.code = Global.SUCCESS_CODE;
      result.msg = "Disconnect success!";
    } else {
      result.code = Global.FAILURE_CODE;
      result.msg = "Not connnected yet!";
    }
    DisconnectResp resp = new DisconnectResp();
    resp.status = result;
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    String str = req.statement;
    return executor.execute(str);
  }
}
