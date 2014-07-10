package com.datatorrent.demos.valuecount;

import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.valuecount.Application;

public class ApplicationTest
{
  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode.runApp(new Application(), 15000);
  }
}