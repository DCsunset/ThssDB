package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
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

import java.util.BitSet;
import java.util.Date;
import java.util.Iterator;

public class IServiceHandler implements IService.Iface {
  private DbCache cache;

  public IServiceHandler(DbCache _cache) {
    this.cache = _cache;
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
    // TODO
    return null;
  }

  @Override
  public DisconnetResp disconnect(DisconnetResp req) throws TException {
    // TODO
    return null;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    // TODO
    return null;
  }

  @Override
  public void Insert(java.nio.ByteBuffer data) throws TException {
    System.out.println("insert,x=");
    for (byte c : data.array()) {
      System.out.println(String.format("byte=%c", c));
    }
    // Get an empty page
    int id = cache.metadata.freePageList.get(0);
    Page page = cache.readPage(id);
    // Insert record into that page
    BitSet bitmap = page.bitmap;
    int index = bitmap.nextClearBit(0);
    page.writeRow(index, data.array());
    // If full, change freepagelist
    cache.metadata.freePageList.remove(0);
    // Write page to cache
    cache.writePage(id, page);
    cache.writeBack();
  }
}
