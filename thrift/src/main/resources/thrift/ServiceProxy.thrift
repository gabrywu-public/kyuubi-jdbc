include "DemoService.thrift"

namespace java com.gabry.kyuubi.thrift.service

service DemoServiceProxy extends DemoService.DemoService {
	binary forward(1:i32 argLen,2:binary arg)
}
