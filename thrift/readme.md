## install thrift

> brew install thrift

## build thrift

1. `thrift -r --gen java --out main/java main/resources/thrift/DemoService.thrift`
2. `thrift --gen java:beans,hashcode -o src/gen/thrift if/TCLIService.thrift`

## references

1.https://static.kancloud.cn/digest/thrift/118984
