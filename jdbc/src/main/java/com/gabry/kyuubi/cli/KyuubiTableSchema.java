package com.gabry.kyuubi.cli;

import org.apache.hive.service.rpc.thrift.TColumnDesc;
import org.apache.hive.service.rpc.thrift.TTableSchema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KyuubiTableSchema {
  // private final TTableSchema tableSchema;
  private final List<KyuubiColumn> columns;
  private final Map<String, KyuubiColumn> columnNameMap;

  public KyuubiTableSchema(TTableSchema tableSchema) {
    //  this.tableSchema = tableSchema;
    this.columns =
        tableSchema.getColumns().stream().map(KyuubiColumn::new).collect(Collectors.toList());
    this.columnNameMap =
        tableSchema.getColumns().stream()
            .collect(Collectors.toMap(TColumnDesc::getColumnName, KyuubiColumn::new));
  }

  public List<KyuubiColumn> getColumns() {
    return columns;
  }

  public KyuubiColumn getColumn(String columnLabel) {
    return columnNameMap.get(columnLabel);
  }

  public KyuubiColumn getColumn(int index) {
    return columns.get(index);
  }

  public int getColumnSize() {
    return columns.size();
  }
}
