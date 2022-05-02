package com.gabry.shadow.kyuubi.driver;

import java.io.IOException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class DriverUtils {
  private DriverUtils() {
    // do nothing
  }

  public static Attributes loadManifestAttributes(Class<?> clazz) throws IOException {
    URL classContainer = clazz.getProtectionDomain().getCodeSource().getLocation();
    URL manifestUrl = new URL("jar:" + classContainer + "!/META-INF/MANIFEST.MF");
    return new Manifest(manifestUrl.openStream()).getMainAttributes();
  }
}
