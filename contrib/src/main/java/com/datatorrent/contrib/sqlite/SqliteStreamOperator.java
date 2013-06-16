/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.datatorrent.lib.util.AbstractSqlStreamOperator;
import com.datatorrent.lib.util.AbstractSqlStreamOperator.InputSchema.ColumnInfo;
import com.malhartech.api.annotation.ShipContainingJars;
import com.malhartech.api.Context.OperatorContext;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
@ShipContainingJars(classes = {com.almworks.sqlite4java.SQLiteConnection.class})
public class SqliteStreamOperator extends AbstractSqlStreamOperator
{
  private static final Logger logger = LoggerFactory.getLogger(SqliteStreamOperator.class);
  protected transient ArrayList<SQLiteStatement> insertStatements = new ArrayList<SQLiteStatement>(5);
  protected transient SQLiteStatement beginStatement;
  protected transient SQLiteStatement commitStatement;
  protected transient SQLiteStatement execStatement;
  protected transient ArrayList<SQLiteStatement> deleteStatements = new ArrayList<SQLiteStatement>(5);
  protected transient SQLiteConnection db;

  @Override
  public void setup(OperatorContext context)
  {
    db = new SQLiteConnection(new File("/tmp/sqlite.db"));
    java.util.logging.Logger.getLogger("com.almworks.sqlite4java").setLevel(java.util.logging.Level.SEVERE);
    SQLiteStatement st;

    try {
      db.open(true);
      // create the temporary tables here
      for (int i = 0; i < inputSchemas.size(); i++) {
        InputSchema inputSchema = inputSchemas.get(i);
        ArrayList<String> indexes = new ArrayList<String>();
        if (inputSchema == null || inputSchema.columnInfoMap.isEmpty()) {
          continue;
        }
        String columnSpec = "";
        String columnNames = "";
        String insertQuestionMarks = "";
        int j = 0;
        for (Map.Entry<String, ColumnInfo> entry : inputSchema.columnInfoMap.entrySet()) {
          if (!columnSpec.isEmpty()) {
            columnSpec += ",";
            columnNames += ",";
            insertQuestionMarks += ",";
          }
          columnSpec += entry.getKey();
          columnSpec += " ";
          columnSpec += entry.getValue().type;
          if (entry.getValue().isColumnIndex) {
            indexes.add(entry.getKey());
          }
          columnNames += entry.getKey();
          insertQuestionMarks += "?";
          entry.getValue().bindIndex = ++j;
        }
        String createTempTableStmt = "CREATE TEMP TABLE " + inputSchema.name + "(" + columnSpec + ")";
        st = db.prepare(createTempTableStmt);
        st.step();
        st.dispose();
        for (String index : indexes) {
          String createIndexStmt = "CREATE INDEX " + inputSchema.name + "_" + index + "_idx ON " + inputSchema.name + " (" + index + ")";
          st = db.prepare(createIndexStmt);
          st.step();
          st.dispose();
        }
        String insertStmt = "INSERT INTO " + inputSchema.name + " (" + columnNames + ") VALUES (" + insertQuestionMarks + ")";

        insertStatements.add(i, db.prepare(insertStmt));
        // We are calling "DELETE FROM" on the tables and because of the "truncate optimization" in sqlite, it should be fast.
        // See http://sqlite.org/lang_delete.html
        deleteStatements.add(i, db.prepare("DELETE FROM " + inputSchema.name));
      }
      beginStatement = db.prepare("BEGIN");
      commitStatement = db.prepare("COMMIT");
      execStatement = db.prepare(statement);
    }
    catch (SQLiteException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    try {
      beginStatement.step();
      beginStatement.reset();
    }
    catch (SQLiteException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void processTuple(int tableNum, HashMap<String, Object> tuple)
  {
    InputSchema inputSchema = inputSchemas.get(tableNum);

    SQLiteStatement insertStatement = insertStatements.get(tableNum);
    try {
      for (Map.Entry<String, Object> entry : tuple.entrySet()) {
        ColumnInfo t = inputSchema.columnInfoMap.get(entry.getKey());
        if (t != null && t.bindIndex != 0) {
          //System.out.println("Binding: "+entry.getValue().toString()+" to "+t.bindIndex);
          insertStatement.bind(t.bindIndex, entry.getValue().toString());
        }
      }

      insertStatement.step();
      insertStatement.reset();
    }
    catch (SQLiteException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void endWindow()
  {
    try {
      commitStatement.step();
      commitStatement.reset();
      if (bindings != null) {
        for (int i = 0; i < bindings.size(); i++) {
          execStatement.bind(i, bindings.get(i).toString());
        }
      }
      int columnCount = execStatement.columnCount();
      while (execStatement.step()) {
        HashMap<String, Object> resultRow = new HashMap<String, Object>();
        for (int i = 0; i < columnCount; i++) {
          resultRow.put(execStatement.getColumnName(i), execStatement.columnValue(i));
        }
        this.result.emit(resultRow);
      }
      execStatement.reset();

      for (SQLiteStatement st : deleteStatements) {
        st.step();
        st.reset();
      }
    }
    catch (SQLiteException ex) {
      throw new RuntimeException(ex);
    }
    bindings = null;
  }

  @Override
  public void teardown()
  {
    db.dispose();
  }

}