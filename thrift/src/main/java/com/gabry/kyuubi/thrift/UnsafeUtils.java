package com.gabry.kyuubi.thrift;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

public class UnsafeUtils {
  private static Unsafe unsafe = null;

  static {
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (Unsafe) unsafeField.get(null);
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  public static long getFieldOffset(Class<?> clazz, String fieldName) throws NoSuchFieldException {
    return unsafe.objectFieldOffset(clazz.getDeclaredField(fieldName));
  }
  public static Object getFieldValue(Object obj,long offset){
    return unsafe.getObject(obj, offset);
  }
}
