package com.datatorrent.demos.valcount;

import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.valuecount.StatefulApplication;

public class ApplicationTest
{
  @Test
  public void testApplication() throws Exception
  {
    /*LocalMode lma = LocalMode.newInstance();
    new StatefulApplication().populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);*/
    LocalMode.runApp(new StatefulApplication(), 15000);
  }
}
