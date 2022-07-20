package com.gabry.kyuubi.thrift.service.impl;

import com.gabry.kyuubi.thrift.service.DemoService;
import org.apache.thrift.TException;

public class DemoServiceImpl implements DemoService.Iface {

  @Override
  public String sayHi(String name) throws TException {
    return "Hi " + name + ", from Thrift Server";
  }
}
