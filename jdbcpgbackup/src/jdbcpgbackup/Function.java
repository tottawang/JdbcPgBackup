package jdbcpgbackup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

final class Function extends DbBackupObject {

  static class FunctionFactory implements DBOFactory<Function> {

    @Override
    public Iterable<Function> getDbBackupObjects(Connection con, Schema schema)
        throws SQLException {
      List<Function> functions = new ArrayList<Function>();
      PreparedStatement stmt = null;
      try {
        stmt = con.prepareStatement("select p.proname as function_name, "
            + "pg_get_userbyid(p.proowner) as owner, "
            + "case when l.lanname = 'internal' then p.prosrc else pg_get_functiondef(p.oid) end as definition "
            + "from pg_proc p left join pg_namespace n on p.pronamespace = n.oid "
            + "left join pg_language l on p.prolang = l.oid where n.nspname= ?");
        stmt.setString(1, schema.getName());
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
          functions.add(new Function(rs.getString("function_name"), schema, rs.getString("owner"),
              rs.getString("definition")));
        }
        rs.close();
      } finally {
        if (stmt != null)
          stmt.close();
      }
      return functions;
    }

    @Override
    public Function getDbBackupObject(Connection con, String functionName, Schema schema)
        throws SQLException {
      Function function = null;
      PreparedStatement stmt = null;
      try {
        stmt = con.prepareStatement("select p.proname as function_name, "
            + "pg_get_userbyid(p.proowner) as owner, "
            + "case when l.lanname = 'internal' then p.prosrc else pg_get_functiondef(p.oid) end as definition "
            + "from pg_proc p left join pg_namespace n on p.pronamespace = n.oid "
            + "left join pg_language l on p.prolang = l.oid where n.nspname= ? and and p.proname= ?");
        stmt.setString(1, schema.getName());
        stmt.setString(2, functionName);
        ResultSet rs = stmt.executeQuery();
        if (rs.next())
          function =
              new Function(functionName, schema, rs.getString("owner"), rs.getString("definition"));
        else
          throw new RuntimeException("no such function: " + functionName);
        rs.close();
      } finally {
        if (stmt != null)
          stmt.close();
      }
      return function;
    }
  }

  static class CachingFunctionFactory extends CachingDBOFactory<Function> {

    protected CachingFunctionFactory(Schema.CachingSchemaFactory schemaFactory) {
      super(schemaFactory);
    }

    @Override
    protected final PreparedStatement getAllStatement(Connection con) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected final Function newDbBackupObject(Connection con, ResultSet rs, Schema schema)
        throws SQLException {
      throw new UnsupportedOperationException();
    }
  }

  private final String definition;

  private Function(String name, Schema schema, String owner, String definition) {
    super(name, schema, owner);
    // remove schema name
    this.definition = definition.replace(schema.getName() + ".", "");
  }

  @Override
  protected StringBuilder appendCreateSql(StringBuilder buf) {
    buf.append(definition);
    return buf;
  }

}
