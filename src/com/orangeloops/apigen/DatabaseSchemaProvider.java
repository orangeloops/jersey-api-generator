package com.orangeloops.apigen;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface DatabaseSchemaProvider {
	
	void setConnectionInformation(String host, String dbname, String user, String password);
	
	List<String> getTableNames() throws SQLException;	
	Map<String,String> getTableColumns(String tableName) throws SQLException;
	Map<String,String> getPrimaryKeysForTable(String tableName) throws SQLException;
	Map<String,String> getNonNullableColumnNames(String tableName) throws SQLException;
	Map<String,String> getForeignKeysForTable(String tableName) throws SQLException;
}
