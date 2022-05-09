package com.gabry.kyuubi.cli;

import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.TypeDescriptor;
import org.apache.hive.service.rpc.thrift.TTableSchema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KyuubiTableSchema extends TableSchema {
  private TableSchema delegate;
  private Map<String, ColumnDescriptor> columnLabelMap;

  public KyuubiTableSchema(TableSchema tableSchema) {
    this.delegate = tableSchema;
    this.columnLabelMap =
        delegate.getColumnDescriptors().stream()
            .collect(Collectors.toMap(ColumnDescriptor::getName, x -> x));
  }

  @Override
  public List<ColumnDescriptor> getColumnDescriptors() {
    return delegate.getColumnDescriptors();
  }

  public ColumnDescriptor getColumnDescriptorOf(String columnLabel) {
    return columnLabelMap.get(columnLabel);
  }

  @Override
  public ColumnDescriptor getColumnDescriptorAt(int pos) {
    return delegate.getColumnDescriptorAt(pos);
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public TTableSchema toTTableSchema() {
    return delegate.toTTableSchema();
  }

  @Override
  public TypeDescriptor[] toTypeDescriptors() {
    return delegate.toTypeDescriptors();
  }

  @Override
  public TableSchema addPrimitiveColumn(String columnName, Type columnType, String columnComment) {
    return delegate.addPrimitiveColumn(columnName, columnType, columnComment);
  }

  @Override
  public TableSchema addStringColumn(String columnName, String columnComment) {
    return delegate.addStringColumn(columnName, columnComment);
  }
}
