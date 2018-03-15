package com.orangeloops.apigen;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresqlSchemaProvider implements DatabaseSchemaProvider {	
	
	private static final String DB_DRIVER = "org.postgresql.Driver";
	private String databaseHost = null;
	private String databaseName = null;
	private String databaseUser = null;
	private String databasePassword = null;	
	
	public void setConnectionInformation(String host, String name, String user, String password) {
		databaseHost = host;
		databaseName = name;
		databaseUser = user;
		databasePassword = password;
	}
	
	private Connection getDBConnection() {
		Connection conn = null;
	    try
	    {
	      Class.forName(DB_DRIVER);
	      if(databaseHost == null) {
	    	  System.err.println("Database host information is missing!");
	      }
	      if(databaseUser == null) {
	    	  System.err.println("Database user information is missing!");
	      }
	      if(databasePassword == null) {
	    	  System.err.println("Database password information is missing!");
	      }
	      
	      String connectionString = "jdbc:postgresql://"+databaseHost+"/"+databaseName;
	      conn = DriverManager.getConnection(connectionString,databaseUser, databasePassword);
	    }
	    catch (ClassNotFoundException e)
	    {
	      e.printStackTrace();
	      System.exit(1);
	    }
	    catch (SQLException e)
	    {
	      e.printStackTrace();
	      System.exit(2);
	    }
	    return conn;
	}
	
	public List<String> getTableNames() throws SQLException{
		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;		
		
		ArrayList<String> tableNames = new ArrayList<String>();
	    try 
	    {
	      dbConnection = getDBConnection();
	      String querySQL = "select * from information_schema.tables where table_type='BASE TABLE' and table_schema='public'";
	      preparedStatement = dbConnection.prepareStatement(querySQL);
		  
		  ResultSet rs = preparedStatement.executeQuery();
	      while ( rs.next() )
	      { 
	    	String tableName = rs.getString("table_name");	        
	    	tableNames.add(tableName);
	      }
	      rs.close();
	      
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		
			if (dbConnection != null) {
				dbConnection.close();
			}
		}	    
	    return tableNames;
	}	
	
	public Map<String,String> getPrimaryKeysForTable(String tableName) throws SQLException {
		
		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;		
		
		Map<String,String> columns = new HashMap<String,String>();
	    try 
	    {
	      dbConnection = getDBConnection();
	      String querySQL = "SELECT pg_attribute.attname, format_type(pg_attribute.atttypid, pg_attribute.atttypmod) "+  
	    		  			"FROM pg_index, pg_class, pg_attribute, pg_namespace "+
	    		  			"WHERE " + 
	    		  			" pg_class.oid = ?::regclass AND " + 
	    		  			" indrelid = pg_class.oid AND " +
	    		  			" nspname = 'public' AND " +
	    		  			" pg_class.relnamespace = pg_namespace.oid AND " + 
	    		  			" pg_attribute.attrelid = pg_class.oid AND " +
	    		  			" pg_attribute.attnum = any(pg_index.indkey) AND indisprimary";
	      preparedStatement = dbConnection.prepareStatement(querySQL);
		  preparedStatement.setString(1, tableName);
		  ResultSet rs = preparedStatement.executeQuery();
	      while ( rs.next() )
	      { 
	        String colName	= rs.getString("attname");
	        String colType	= rs.getString("format_type");
	        columns.put(colName,colType);
	        
	      }
	      rs.close();
	      
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		
			if (dbConnection != null) {
				dbConnection.close();
			}
		}	    
	    return columns;		
	}
	
	public Map<String,String> getForeignKeysForTable(String tableName) throws SQLException {
		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;		
		
		Map<String,String> columns = new HashMap<String,String>();
		try 
		{
			dbConnection = getDBConnection();
		  
			String querySQL = 
							"select att2.attname as \"child_column\", cl.relname as \"parent_table\", att.attname as \"parent_column\" "
							+" from "
							+"	(select unnest(con1.conkey) as \"parent\", unnest(con1.confkey) as \"child\", con1.confrelid, con1.conrelid "
							+"	from pg_class cl "
							+" join pg_namespace ns on cl.relnamespace = ns.oid "
							+" join pg_constraint con1 on con1.conrelid = cl.oid "
							+" where  cl.relname = ? and ns.nspname = 'public' and con1.contype = 'f' "
							+" ) con "
							+" join pg_attribute att on att.attrelid = con.confrelid and att.attnum = con.child "
							+" join pg_class cl on cl.oid = con.confrelid "
							+" join pg_attribute att2 on att2.attrelid = con.conrelid and att2.attnum = con.parent ";
			

			preparedStatement = dbConnection.prepareStatement(querySQL);
			preparedStatement.setString(1, tableName);
			ResultSet rs = preparedStatement.executeQuery();
			while ( rs.next() )
			{ 
				String colName	= rs.getString("column_name");
				String colType	= rs.getString("data_type");
				columns.put(colName, colType);
			}
			rs.close();
		  
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		
			if (dbConnection != null) {
				dbConnection.close();
			}
		}	    
		return columns;
	}
	
	public Map<String,String> getNonNullableColumnNames(String tableName) throws SQLException{
		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;		
		
		Map<String,String> columns = new HashMap<String,String>();
	    try 
	    {
	      dbConnection = getDBConnection();
	      String querySQL = "select column_name,data_type from information_schema.columns where table_name=? AND is_nullable='NO'";
	      preparedStatement = dbConnection.prepareStatement(querySQL);
		  preparedStatement.setString(1, tableName);
		  ResultSet rs = preparedStatement.executeQuery();
	      while ( rs.next() )
	      { 
	        String colName	= rs.getString("column_name");
	        String colType	= rs.getString("data_type");
	        columns.put(colName, colType);
	        
	      }
	      rs.close();
	      
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		
			if (dbConnection != null) {
				dbConnection.close();
			}
		}	    
	    return columns;
	}	
	
	public Map<String,String> getTableColumns(String tableName) throws SQLException{
		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;		
		
		Map<String,String> columns = new HashMap<String,String>();
	    try 
	    {
	      dbConnection = getDBConnection();
	      String querySQL = "select column_name,data_type from information_schema.columns where table_name=?";
	      preparedStatement = dbConnection.prepareStatement(querySQL);
		  preparedStatement.setString(1, tableName);
		  ResultSet rs = preparedStatement.executeQuery();
	      while ( rs.next() )
	      { 
	        String colName	= rs.getString("column_name");
	        String colType	= rs.getString("data_type");
	        columns.put(colName, colType);
	        
	      }
	      rs.close();
	      
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		
			if (dbConnection != null) {
				dbConnection.close();
			}
		}	    
	    return columns;
	}
}

