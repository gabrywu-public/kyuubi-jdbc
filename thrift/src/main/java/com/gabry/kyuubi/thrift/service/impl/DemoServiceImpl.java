package com.gabry.kyuubi.thrift.service.impl;

import com.gabry.kyuubi.thrift.service.DemoService;
import org.apache.thrift.TException;

public class DemoServiceImpl implements DemoService.Iface {
  private final String serviceName;
  public DemoServiceImpl(String serviceName){
    this.serviceName = serviceName;
  }
  private DemoServiceImpl(){
    this.serviceName = null;
  }
  @Override
  public String sayHi(String name) throws TException {
    return "Hi " + name + ", from " + serviceName;
  }
}
