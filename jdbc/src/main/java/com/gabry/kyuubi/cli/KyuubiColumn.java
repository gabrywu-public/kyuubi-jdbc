package com.gabry.kyuubi.cli;

import org.apache.hive.service.rpc.thrift.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.gabry.kyuubi.cli.KyuubiColumnType.DECIMAL_TYPE;

public class KyuubiColumn {
  private final String name;
  private final String comment;
  private final int position;
  private final KyuubiColumnType columnType;
  private final Optional<TypeQualifiers> typeQualifiers;

  public KyuubiColumn(TColumnDesc columnDesc) {
    this.name = columnDesc.getColumnName();
    this.comment = columnDesc.getComment();
    this.position = columnDesc.getPosition();
    List<TTypeEntry> tTypeEntries = columnDesc.getTypeDesc().getTypes();
    TPrimitiveTypeEntry top = tTypeEntries.get(0).getPrimitiveEntry();
    this.columnType = KyuubiColumnType.getType(top.getType());
    this.typeQualifiers =
        top.isSetTypeQualifiers()
            ? Optional.of(new TypeQualifiers(top.getTypeQualifiers()))
            : Optional.empty();
  }

  public String getName() {
    return name;
  }

  public String getComment() {
    return comment;
  }

  public int getPosition() {
    return position;
  }

  public Optional<TypeQualifiers> getQualifiers() {
    return typeQualifiers;
  }

  public KyuubiColumnType getType() {
    return columnType;
  }

  public Integer getPrecision() {
    if (this.columnType == DECIMAL_TYPE) {
      return typeQualifiers.get().getPrecision();
    }
    return this.columnType.getMaxPrecision();
  }

  public Integer getDecimalDigits() {
    switch (this.columnType) {
      case BOOLEAN_TYPE:
      case TINYINT_TYPE:
      case SMALLINT_TYPE:
      case INT_TYPE:
      case BIGINT_TYPE:
        return 0;
      case FLOAT_TYPE:
        return 7;
      case DOUBLE_TYPE:
        return 15;
      case DECIMAL_TYPE:
        return typeQualifiers.map(TypeQualifiers::getScale).orElse(null);
      case TIMESTAMP_TYPE:
        return 9;
      default:
        return null;
    }
  }

  public Integer getColumnSize() {
    if (columnType.isNumericType()) {
      return getPrecision();
    }
    switch (columnType) {
      case STRING_TYPE:
      case BINARY_TYPE:
        return Integer.MAX_VALUE;
      case CHAR_TYPE:
      case VARCHAR_TYPE:
        return typeQualifiers.map(TypeQualifiers::getCharacterMaximumLength).orElse(null);
      case DATE_TYPE:
        return 10;
      case TIMESTAMP_TYPE:
        return 29;
      case TIMESTAMPLOCALTZ_TYPE:
        return 31;
      default:
        return null;
    }
  }

  public static class TypeQualifiers {
    private Integer characterMaximumLength;
    private Integer precision;
    private Integer scale;

    public TypeQualifiers(TTypeQualifiers typeQualifiers) {
      Map<String, TTypeQualifierValue> tqMap = typeQualifiers.getQualifiers();

      if (tqMap.containsKey(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH)) {
        characterMaximumLength =
            tqMap.get(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH).getI32Value();
      }

      if (tqMap.containsKey(TCLIServiceConstants.PRECISION)) {
        precision = tqMap.get(TCLIServiceConstants.PRECISION).getI32Value();
      }

      if (tqMap.containsKey(TCLIServiceConstants.SCALE)) {
        scale = tqMap.get(TCLIServiceConstants.SCALE).getI32Value();
      }
    }

    public Integer getCharacterMaximumLength() {
      return characterMaximumLength;
    }

    public Integer getPrecision() {
      return precision;
    }

    public Integer getScale() {
      return scale;
    }
  }
}
