package com.orangeloops.apigen;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import java.nio.file.Paths;

public class Generator {
	
	public static String _version = "1.0";
	public static String basePackageName = "com.yourcompany.yourproduct";
	public static String outputDAOsFolder = "output";
	public static String outputBeansFolder = "output";
	public static String outputRESTFolder = "output";

	public static void main(String[] args) {
		
		System.out.println("Starting Jersey REST API Endpoint Generator v"+_version);
		
		try {
			
			Map<String,String> arguments = processCommandLineArguments(args);
			
			DatabaseSchemaProvider schemaProvider = new PostgresqlSchemaProvider();
			schemaProvider.setConnectionInformation(arguments.get("host"), arguments.get("name"),arguments.get("user"), arguments.get("password"));
			
			List<String> tables = schemaProvider.getTableNames();
			Set<String> tablesToInclude = getTablesToInclude();			
			
			String baseOutputPath = Paths.get(".").toAbsolutePath().normalize().toString();
			String singleTable = arguments.containsKey("table") ? arguments.get("table") : null;
			
			System.out.println("Generating files to " + baseOutputPath);
			for(int i=0;i<tables.size();i++){
				String tableName = tables.get(i);
				System.out.println("Generating files for "+ tableName + "...");
				if (!tablesToInclude.contains(tableName) || (singleTable.length() > 0 && !singleTable.equals(tableName))) {
					System.out.println("Table "+tableName+" is ignored, skipping.");
					continue;
				}
				try {
					Map<String,String> columns = schemaProvider.getTableColumns(tableName);
					Map<String,String> keysMap = schemaProvider.getPrimaryKeysForTable(tableName);
					List<String> keys = new ArrayList<>(keysMap.keySet());
					
					//Generate Java Beans
					String path = baseOutputPath + "/" + outputBeansFolder + "/src/com/yourcompany/yourproduct/datatypes/gen/";
					generateBeanForTable(path, tableName, columns, keys);
					
					//Generate DAOs
					path = baseOutputPath + "/" + outputDAOsFolder + "/src/com/yourcompany/yourproduct/dao/gen/";
					generateDaoForTable(path, tableName, columns, keys);
					
					//Generate REST Facades
					path = baseOutputPath + "/" + outputRESTFolder + "/src/com/yourcompany/yourproduct/rest/gen/";
					generateRestFacadeForTable(path, tableName, columns, keys);
					
				} catch(Exception exc){
					System.err.println("Exception processing table "+tableName+" Error Message:"+exc.getMessage());
					exc.printStackTrace(System.err);
				}			
			}			
		} catch(Exception ex){
			System.err.println("Unexpected exception running class generation. Message:"+ex.getMessage());
			ex.printStackTrace(System.err);
		}
		System.out.println("Generation finished");
	}
	
	private static Map<String,String> processCommandLineArguments(String[] args){
		if(args.length<3){
			System.out.println("Error: Expected arguments missing!");
			System.out.println("Usage:");
			System.out.println("java com.orangeloops.apigen.Generator [database host] [database name] [database user] [database password] [table name] ");
			System.exit(0);
		}
		Map<String,String> argsMap = new HashMap<String,String>();
		argsMap.put("host", args[0]);
		argsMap.put("name", args[1]);
		argsMap.put("user", args[2]);
		argsMap.put("password", args[3]);
		
		if(args.length > 4) {
			argsMap.put("table", args[4]);
		}
		
		return argsMap;
	}
	
	private static void generateRestFacadeForTable(String path,
			String tableName, Map<String, String> columns, List<String> keys) throws IOException {
		
		String txt = getTemplate("Rest.temp");
		String className = getPascalCaseName(tableName);
		
		Set<String> exceptions = new HashSet<String>();
		exceptions.add("id");
		exceptions.add("created_date");
		exceptions.add("created_by");
		exceptions.add("created_by_name");
		exceptions.add("deleted_date");
		exceptions.add("deleted_by");
		exceptions.add("deleted_by_name");
		exceptions.add("is_deleted");		
		String commaSeparatedQuotedFields = getCommaSeparatedQuotedFieldNames(exceptions,columns);
		String loadMapWithNonNullFields = getMapLoadWithNonNullFields(exceptions,columns);
		
		//Replace <$CLASSNAME$>
		String temp = txt.replaceAll("<#CLASSNAME#>", className);
		
		//Replace <#BASEPACKAGE#>
		temp = temp.replaceAll("<#BASEPACKAGE#>", basePackageName);	
		
		//Replace <$TABLENAME$>
		String urlName = tableName.replace("_", "-");
		temp = temp.replaceAll("<#URLNAME#>", urlName);
		temp = temp.replaceAll("<#COMMA_SEPARATED_QUOTED_FIELDS#>", commaSeparatedQuotedFields);
		temp = temp.replaceAll("<#LOAD_MAP_WITH_NONNULL_FIELDS#>", loadMapWithNonNullFields);
		temp = processIFStatements(tableName, columns, temp);
		
		String fileName = path +className+"BaseREST.java";
		checkDirectoryIsPresent(fileName);
		PrintWriter out = new PrintWriter(fileName);
		out.print(temp);
		System.out.println("File written for class "+ fileName);
		out.close();		
	}

	private static String processIFStatements(String tableName, Map<String,String> columns, String txt) throws IOException {
		String temp = txt;
		int index = temp.indexOf("<#IF");
		String prevBlock = "";
		while(index>0){
			prevBlock = temp.substring(0,index);
			String sub = temp.substring(index + (new String("<#IF")).length());
			String expr = getExpression(sub);
			boolean includeIfBlock = evaluateBoolExpression(expr, tableName, columns);
			if(includeIfBlock){
				int ifClose = temp.indexOf("#>");				
				int endIf = temp.indexOf("<#ENDIF#>");
				if(endIf<=0){
					throw new IllegalArgumentException("[includeIfBlock:true] Template lacks ENDIF statement for opening IF "+expr);
				}				
				String block = temp.substring(ifClose+2,endIf);
				String afterBlock = temp.substring(endIf + 9);
				temp = prevBlock + block + afterBlock;
			} else {				
				int endIf = temp.indexOf("<#ENDIF#>");
				if(endIf<=0){
					throw new IllegalArgumentException("[includeIfBlock:false] Template lacks ENDIF statement for opening IF "+expr);
				}				
				String afterBlock = temp.substring(endIf + 9);
				temp = prevBlock + afterBlock;												
			}		
			index = temp.indexOf("<#IF");			
		}
		return temp;
	}
	
	private static String getExpression(String txt) {
		int index = txt.indexOf("#>");
		if(index>0){
			String expr = txt.substring(0, index);
			return expr.trim();
		} else {
			throw new IllegalArgumentException("Closing IF statement #> markup not found. IF:"+txt);
		}
	}
	
	private static boolean evaluateBoolExpression(String expr, String tableName, Map<String,String> columns) throws IOException {
		if(expr.equals("NO_CUSTOM_REST")){
			try {
				Set<String> customRest = getRestCustomizations();
				return !(customRest.contains(tableName));
			} catch(IOException io){
				System.err.println("Exception trying to read CustomizedRestEndpoints.txt file. Message:"+ io.getMessage());
			}
		} else if(expr.equals("HAS_CREATEDBY")) {
			boolean hasCreatedColumn = columns.containsKey("created_by");
			return hasCreatedColumn;
		} else if(expr.equals("HAS_MODIFIEDBY")) {
			boolean hasModifiedColumn = columns.containsKey("modified_by");
			return hasModifiedColumn;			
		} else if(expr.equals("SOFT_DELETE")) {
			boolean softDelete = columns.containsKey("is_deleted");
			return softDelete;
		} else {
			System.err.println("Unrecognized expression "+ expr +" ... ignored");
		}
		return false;
	}

	private static String getMapLoadWithNonNullFields(Set<String> exceptions,
			Map<String, String> columns) {
		
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = columns.keySet().iterator();
		while(iter.hasNext()){
			String colName = iter.next();
			if(!exceptions.contains(colName)){
				String fieldCamel = getCamelCaseName(colName);
				String fieldPascal = getPascalCaseName(colName);
				builder.append("\t\tvalue = bean.get"+fieldPascal+"();");	
				builder.append(System.getProperty("line.separator"));
				builder.append("\t\tif(value!=null){");
				builder.append(System.getProperty("line.separator"));
				builder.append("\t\t\tmap.put(\""+fieldCamel+"\",value);");
				builder.append(System.getProperty("line.separator"));	
				builder.append("\t\t}");
				builder.append(System.getProperty("line.separator"));
			}
		}
		builder.deleteCharAt(builder.length()-1);
		return builder.toString();
	}
	
	private static void checkDirectoryIsPresent(String fileName) {
		File file = new File(fileName);
		if(!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();	
		}		
	}

	public static void generateBeanForTable(String path, String tableName, Map<String,String> columns, List<String> primaryKeys) throws IOException
	{
		String txt = getTemplate("Bean.temp");
		String className = getPascalCaseName(tableName);
		
		//Replace <#CLASSNAME#>
		String temp = txt.replaceAll("<#CLASSNAME#>", className);
		
		//Replace <#BASEPACKAGE#>
		temp = temp.replaceAll("<#BASEPACKAGE#>", basePackageName);		
		
		Iterator<String> iter = columns.keySet().iterator();
		StringBuilder builder = new StringBuilder();
		while(iter.hasNext()){
			String colName = iter.next();
			String colType = columns.get(colName);
			String tmp = generateBeanSnippetForColumn(colName, colType);
			builder.append(tmp);
		}
		
		temp = temp.replaceAll("<#op#>", builder.toString());
		
		//Replace <#JSONOBJECT_TO_BEAN#>
		temp = temp.replaceAll("<#JSONOBJECT_TO_BEAN#>", generateBeanSnippetForJSONTOBEAN(className, columns));		
		temp = temp.replaceAll("<#BEAN_TO_JSONOBJECT#>", generateBeanSnippetForBEANTOJSON(className, columns));		
		
		String fileName = path +className+".java";
		checkDirectoryIsPresent(fileName);
		PrintWriter out = new PrintWriter(fileName);
		out.print(temp);
		System.out.println("File written for class "+ fileName);
		out.close();		
	}
	
	public static String generateBeanSnippetForJSONTOBEAN(String className, Map<String,String> columns) 
	{
		StringBuilder builder = new StringBuilder();
		
		builder.append(className+" bean = new "+className+"();"+System.getProperty("line.separator"));
		
		Iterator<String> keys = columns.keySet().iterator();
		while(keys.hasNext()) {
			String key = keys.next();
			String colType = columns.get(key);
			String camelName = getCamelCaseName(key);
			if(colType.startsWith("character")||colType.equals("text")||colType.equals("xml")||colType.equals("json")||colType.equals("jsonb")){
				builder.append("\t\tif(jObj.has(\""+camelName+"\"))bean."+camelName+" = jObj.getString(\""+camelName+"\");"+System.getProperty("line.separator"));
			} else if(colType.equals("smallint")||colType.equals("integer")){
				builder.append("\t\tif(jObj.has(\""+camelName+"\"))bean."+camelName+" = jObj.getInt(\""+camelName+"\");"+System.getProperty("line.separator"));
			} else if(colType.equals("bigint")){
				builder.append("\t\tif(jObj.has(\""+camelName+"\"))bean."+camelName+" = jObj.getLong(\""+camelName+"\");"+System.getProperty("line.separator"));
			} else if(colType.equals("real")||colType.equals("numeric")||colType.equals("double precision")||colType.equals("money")){
				builder.append("\t\tif(jObj.has(\""+camelName+"\"))bean."+camelName+" = jObj.getDouble(\""+camelName+"\");"+System.getProperty("line.separator"));
			} else if(colType.equals("boolean")||colType.equals("bit")){
				builder.append("\t\tif(jObj.has(\""+camelName+"\"))bean."+camelName+" = jObj.getBoolean(\""+camelName+"\");"+System.getProperty("line.separator"));
			} else if(colType.equals("date") || colType.startsWith("timestamp")){
				builder.append("\t\tif(jObj.has(\""+camelName+"\"))bean."+camelName+" = jObj.getLong(\""+camelName+"\");"+System.getProperty("line.separator"));
			} else {
				throw new IllegalArgumentException("Unknown column type, type name: "+colType);
			}			
		}
		return builder.toString();
	}	
	
	public static String generateBeanSnippetForBEANTOJSON(String className, Map<String,String> columns) 
	{
		StringBuilder builder = new StringBuilder();
		
		builder.append(System.getProperty("line.separator"));
		
		Iterator<String> keys = columns.keySet().iterator();
		while(keys.hasNext()) {
			String key = keys.next();
			String camelName = getCamelCaseName(key);
			builder.append("\t\tjObj.put(\""+camelName+"\", this."+camelName+");"+System.getProperty("line.separator"));			
		}
		return builder.toString();
	}	
	
	public static String getJSONTypeForColumnType(String colType)
	{
		if(colType.startsWith("character")||colType.equals("text")||colType.equals("xml")||colType.equals("json")||colType.equals("jsonb")){
			return "String";
		} else if(colType.equals("smallint")||colType.equals("integer")){
			return "Int";
		} else if(colType.equals("bigint")){
			return "Long";
		} else if(colType.equals("real")||colType.equals("numeric")||colType.equals("double precision")||colType.equals("money")){
			return "Double";
		} else if(colType.equals("boolean")||colType.equals("bit")){
			return "Boolean";
		} else if(colType.equals("date")){
			return "Date";
		} else if(colType.startsWith("timestamp")){
			return "Timestamp";
		} else {
			throw new IllegalArgumentException("Unknown column type, type name: "+colType);
		}
	}	
	
	public static String generateBeanSnippetForColumn(String columnName, String columnType) 
	{
		StringBuilder builder = new StringBuilder();
		String dataType = getClassNameForColumnType(columnType);
		String camelCaseName = getCamelCaseName(columnName);
		String pascalCaseName = getPascalCaseName(columnName);
	
		//builder.append("\t//@Column(name=\""+ columnName + "\")");
		builder.append("\tprivate "+ dataType + " "+ camelCaseName + ";");
		builder.append(System.getProperty("line.separator"));
		builder.append("\tpublic "+dataType+" get"+pascalCaseName+"(){ return this."+camelCaseName+"; }");
		builder.append(System.getProperty("line.separator"));
		builder.append("\tpublic void set"+pascalCaseName+"("+dataType+" param){ this."+camelCaseName+"=param; }");
		builder.append(System.getProperty("line.separator"));
		builder.append(System.getProperty("line.separator"));
		return builder.toString();
	}
	
	public static void generateDaoForTable(String path, String tableName, Map<String,String> columns, List<String> primaryKeys) throws IOException{
		String txt = getTemplate("JdbcDao.temp");
		String className = getPascalCaseName(tableName);
		if(primaryKeys.size()!= 1){
			throw new IllegalArgumentException("Table "+tableName+" does not comply with DAO Generator requirements. Primary Keys != 1.");
		} 
		String pk = primaryKeys.get(0);
		String colType = columns.get(pk);
		boolean pkIsString = colType.startsWith("character"); 
		
		//Replace <$CLASSNAME$>
		String temp = txt.replaceAll("<#CLASSNAME#>", className);
		//Replace <#BASEPACKAGE#>
		temp = temp.replaceAll("<#BASEPACKAGE#>", basePackageName);			
		//Replace <$TABLENAME$>
		temp = temp.replaceAll("<#TABLENAME#>", tableName);
		//Replace <$PRIMARY_KEY$>
		temp = temp.replaceAll("<#PRIMARY_KEY#>", pk);		
		//Replace <#VAR_ID#>
		temp = temp.replaceAll("<#VAR_ID#>", getVarIdDeclaration(pkIsString));
		//Replace <#HAS_MODIFIED_DATE_FIELD#>
		temp = temp.replaceAll("<#HAS_MODIFIED_DATE_FIELD#>", columns.containsKey("modified_date")?"true":"false");
		//Replace <#PREPARE_STATEMENT_PK#>
		temp = temp.replaceAll("<#PREPARE_STATEMENT_PK#>", getPrepareStatementPK(pkIsString));
		//Replace <$COMMA_SEPARATED_COLUMNS$>
		temp = temp.replaceAll("<#COMMA_SEPARATED_COLUMNS#>", getCommaSeparatedColumnNames(pkIsString, pk, columns));
		//Replace <$COMMA_SEPARATED_?$>
		String replacement = getCommaSeparatedQuestionMarks(pkIsString, columns);
		temp = temp.replaceAll("<#COMMA_SEPARATED_QM#>", replacement);		
		//Replace <$PREPARE_STATEMENT$>
		temp = temp.replaceAll("<#PREPARE_STATEMENT#>", getPrepareStatements(pkIsString, pk, columns));	
		//Replace <$POPULATE_BEAN_FROM_RESULTSET$>
		temp = temp.replaceAll("<#POPULATE_BEAN_FROM_RESULTSET#>", getPopulateBeanFromResultSet(pk, columns));
		//Replace <#FIELD_COLUMN_MAPPING#>
		temp = temp.replaceAll("<#FIELD_COLUMN_MAPPING#>", getPopulateFieldColumnMapping(columns));
		//<#ORDER_BY_SQL#>
		temp = temp.replaceAll("<#ORDER_BY_SQL#>", getOrderByStatement(columns));
		//(is_deleted is NULL OR is_deleted=false)
		temp = temp.replaceAll("<#WHERE_FILTERS#>", getWhereFilters(columns, false));
		//WHERE_FILTERS_SINCE
		temp = temp.replaceAll("<#WHERE_FILTERS_SINCE#>", getWhereFilters(columns, true));
		//<#URLNAME#>
		String urlName = tableName.replace("_", "-");
		temp = temp.replaceAll("<#URLNAME#>", urlName);		
		
		String listTemplate = getListMethodTemplate(temp);
		temp = extractListMethodTemplate(temp);
		//(is_deleted is NULL OR is_deleted=false)
		temp = temp.replaceAll("<#CUSTOM_LIST_METHODS#>", processCustomListMethods(listTemplate, tableName));
		
		String defaultDeleteInvocation = columns.containsKey("is_deleted")? "softDelete(id,deletedBy,spaceId,orgId);":"hardDelete(id,deletedBy,spaceId,orgId);";
		temp = temp.replaceAll("<#DEFAULT_DELETE#>", defaultDeleteInvocation);
		
		//<#LOG_INSERT#>
		//<#LOG_DELETE#>
		//if you want to enable transaction logging
		/*
		if(getFileLinesAsSet("/resources/TransactionLogEntity.txt").contains(tableName)){
			temp = temp.replaceAll("<#LOG_INSERT#>", "logTransaction(getInsertTransactionBean(bean), dbConnection);");
			temp = temp.replaceAll("<#LOG_DELETE#>", "logTransaction(getDeleteTransactionBean(id,userId,spaceId,orgId), dbConnection);");
			temp = temp.replaceAll("<#MUST_LOG_TRANSACTION#>", "getUpdateTransactionBean(id, modifiedBy.getId(), spaceId, organizationId,updates)");
		} else { 
			*/
			temp = temp.replaceAll("<#LOG_INSERT#>", "");
			temp = temp.replaceAll("<#LOG_DELETE#>", "");
			temp = temp.replaceAll("<#MUST_LOG_TRANSACTION#>", "null");
		//}
		
		temp = processIFStatements(tableName, columns, temp);
		
		String fileName = path +className+"BaseDAO.java";
		checkDirectoryIsPresent(fileName);
		PrintWriter out = new PrintWriter(fileName);
		out.print(temp);
		System.out.println("File written for class "+ fileName);
		out.close();
	}
	
	private static String processCustomListMethods(String listTemplate, String tableName) throws IOException {

		Set<String> txt = getCustomListMethods(tableName);
		Iterator<String> lines = txt.iterator();
		StringBuilder builder = new StringBuilder();
		while(lines.hasNext()){
			String line = lines.next();
			String [] terms = line.split(":", 3);
			String methodBody = getListMethod(terms[1], terms[2], listTemplate);
			builder.append(methodBody);
			builder.append(System.getProperty("line.separator")+System.getProperty("line.separator"));
		}
		return builder.toString();
	}
	
	private static String getListMethod(String methodName, String params, String template) {
		//<#LIST_METHOD_NAME#>
		//<#LIST_METHOD_PARAMS#>
		//<#LIST_WHERE_STATEMENT#>
		//<#LIST_SET_PARAMS#>
		List<String> columnNames = getColumnNames(params);
		List<String> fieldNames = getFieldNames(params);
		String methodBody = template;
		methodBody = methodBody.replaceAll("<#LIST_METHOD_NAME#>", methodName);
		methodBody = methodBody.replaceAll("<#LIST_METHOD_PARAMS#>", "String "+ getGluedItemsAsString(fieldNames,", String "));
		methodBody = methodBody.replaceAll("<#LIST_WHERE_STATEMENT#>",getGluedItemsAsString(columnNames,"=? AND ")+"=? ");
		methodBody = methodBody.replaceAll("<#LIST_SET_PARAMS#>", getPreparedStatments(fieldNames));
		return methodBody;
	}
	
	private static String getPreparedStatments(List<String> fields){
		StringBuilder builder = new StringBuilder();
		for(String str:fields){
			builder.append("\t\t\tpreparedStatement.setString(paramIndex++, "+str+");");
			builder.append("\n");
		}
		builder.append("\t\t\tpreparedStatement.setLong(paramIndex++, deletedSince);");
		builder.append("\n");		
		return builder.toString();
	}
	
	private static String getGluedItemsAsString(List<String> items, String glueText){
		StringBuilder builder = new StringBuilder();
		for(String str : items){
			builder.append(str);
			builder.append(glueText);
		}
		String txt = builder.substring(0, builder.length()-glueText.length());
		return txt;
	}

	private static List<String> getColumnNames(String commaSeparatedTableColumns) {
		String [] tableColumns = commaSeparatedTableColumns.split(",");
		ArrayList<String> fieldNames = new ArrayList<String>();
		for(String column : tableColumns){
			fieldNames.add(column.trim());
		}
		return fieldNames;
	}	
	
	private static List<String> getFieldNames(String commaSeparatedTableColumns) {
		String [] tableColumns = commaSeparatedTableColumns.split(",");
		ArrayList<String> fieldNames = new ArrayList<String>();
		for(String column : tableColumns){
			String fieldName = getCamelCaseName(column.trim());
			fieldNames.add(fieldName);
		}
		return fieldNames;
	}

	private static String getListMethodTemplate(String temp){
		int startIndex = temp.indexOf("<#BEGIN_CUSTOM_LIST_TEMPLATE#>")+ "<#BEGIN_CUSTOM_LIST_TEMPLATE#>".length();
		int endIndex = temp.indexOf("<#END_CUSTOM_LIST_TEMPLATE#>");
		String listTemplate = temp.substring(startIndex,endIndex);
		
		return listTemplate;		
	}
	
	//<#BEGIN_CUSTOM_LIST_TEMPLATE#>
	// Get everything in between and remove it from original string
	//<#END_CUSTOM_LIST_TEMPLATE#>	
	private static String extractListMethodTemplate(String temp) {
		int startIndex = temp.indexOf("<#BEGIN_CUSTOM_LIST_TEMPLATE#>");
		int endIndex = temp.indexOf("<#END_CUSTOM_LIST_TEMPLATE#>")+ "<#END_CUSTOM_LIST_TEMPLATE#>".length();
		String cleanText = temp.substring(0,startIndex) + temp.substring(endIndex);
		return cleanText;
	}

	private static String getOrderByStatement(Map<String, String> columns) {
		
		if(columns.containsKey("title")) {
			return " ORDER BY title ";
		} else if(columns.containsKey("created_date")){
			return " ORDER BY created_date DESC ";
		} else if(columns.containsKey("id")) {
			return " ORDER BY id DESC ";
		} else {
			throw new IllegalArgumentException("table does not contain title, created_date, or id as columns");
		}
	}

	private static String getWhereFilters(Map<String, String> columns, boolean includeDeletedSince) {
		if(columns.containsKey("is_deleted")){
			if(includeDeletedSince) {
				return " AND (is_deleted=false OR is_deleted is NULL OR deleted_date>?) ";
			} else {
				return " AND (is_deleted=false OR is_deleted is NULL) ";
			}
		}
		return "";
	}

	private static String getPrepareStatementPK(boolean pkIsString) {
		if(pkIsString){
			return "preparedStatement.setString(1, id);";
		} else {
			return "";
		}
	}

	private static String getPopulateFieldColumnMapping(Map<String, String> columns) {
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = columns.keySet().iterator();
		while(iter.hasNext()){
			String columnName = iter.next(); 
            builder.append("\t\tdbMapping.put(\""+getCamelCaseName(columnName)+"\", \""+columnName+"\");");
            builder.append(System.getProperty("line.separator"));			
		}		
		return builder.toString();
	}
	
	public static Set<String> getFileLinesAsSet(String fileName) throws IOException {
		Set<String> list = new HashSet<String>();
		PostgresqlSchemaProvider dao = new PostgresqlSchemaProvider();
		InputStream input = dao.getClass().getResourceAsStream(fileName);
		if(input==null){
			throw new InvalidParameterException("Invalid filename "+ fileName);
		}
		
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));

        String line;
        while ((line = reader.readLine()) != null) {
        	list.add(line);
        }  
        reader.close();		
        return list;			
	}
	
	public static Set<String> getTablesToInclude() throws IOException {
		return getFileLinesAsSet("/resources/TablesToInclude.txt");		
	}	
	
	public static Set<String> getRestCustomizations() throws IOException {
		return getFileLinesAsSet("/resources/CustomizedRestEndpoints.txt");
	}
	
	public static Set<String> getCustomListMethods(String tableName) throws IOException {
		Set<String> list = new HashSet<String>();
		PostgresqlSchemaProvider dao = new PostgresqlSchemaProvider();
		InputStream input = dao.getClass().getResourceAsStream("/resources/ListMethodsToOverload.txt");
		
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));

        String line;
        while ((line = reader.readLine()) != null) {
        	if(line.startsWith(tableName+":")){
        		list.add(line);
        	}
        }  
        reader.close();		
        return list;		
	}	

	public static String getTemplate(String fileName) throws IOException {
		PostgresqlSchemaProvider dao = new PostgresqlSchemaProvider();
		InputStream input = dao.getClass().getResourceAsStream("/resources/"+fileName);
		if(input==null){
			throw new InvalidParameterException("Unable to read template "+ fileName);
		}
		
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        StringBuilder out = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            out.append(line);
            out.append(System.getProperty("line.separator"));
        }  
        reader.close();		
        return out.toString();
	}
	
	public static String getCommaSeparatedColumnNames(boolean isPkString, String primaryKeyCol, Map<String,String> columns)
	{
		StringBuilder builder = new StringBuilder();
		if(isPkString) {
			builder.append(primaryKeyCol+",");
		}
		Iterator<String> iter = columns.keySet().iterator();
		while(iter.hasNext()){
			String colName = iter.next();
			
			if(!colName.equals(primaryKeyCol)){
				builder.append(colName+",");	
			}
		}
		builder.deleteCharAt(builder.length()-1);
		return builder.toString();
	}
	
	public static String getCommaSeparatedQuotedFieldNames(Set<String> exceptions, Map<String,String> columns)
	{
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = columns.keySet().iterator();
		while(iter.hasNext()){
			String colName = iter.next();
			if(!exceptions.contains(colName)){
				String fieldName = getCamelCaseName(colName);
				builder.append("\""+fieldName+"\",");	
			}
		}
		builder.deleteCharAt(builder.length()-1);
		return builder.toString();
	}	
	
	public static String getCommaSeparatedQuestionMarks(boolean isPkString,Map<String,String> columns){
		StringBuilder builder = new StringBuilder();
		int count=0;
		Iterator<String> iter = columns.keySet().iterator();
		while(iter.hasNext()){
			String colName = iter.next();
			count++;
		}
		int size = count;
		if(!isPkString) {
			size--;
		}
		
		for(int i=0;i<size;i++){
			builder.append("?,");
		}
		builder.deleteCharAt(builder.length()-1);
		return builder.toString();		
	}
	
	public static String getPopulateBeanFromResultSet(String primaryKeyCol, Map<String,String> columns)
	{
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = columns.keySet().iterator();
		
		while(iter.hasNext()){
			String colName = iter.next();
			String tableDataType = columns.get(colName);
			String className = getClassNameForColumnType(tableDataType);
			String propertyName = getPascalCaseName(colName);

			if(tableDataType.equals("integer") || tableDataType.equals("smallint")) {
				builder.append("\t\t\tbean.set"+propertyName+"(getInteger(rs,\""+colName+"\"));");	
				builder.append(System.getProperty("line.separator"));				
			} else {
				builder.append("\t\t\tbean.set"+propertyName+"(rs.get"+ className +"(\""+colName+"\"));");	
				builder.append(System.getProperty("line.separator"));
			}
		}	
		
		return builder.toString();		
	}
	
	public static String getVarIdDeclaration(boolean pkIsString){
		if(pkIsString){
			return "String id = bean.getId()!=null? bean.getId() : SFUUID.shortUUID();";
		} else {
			return "String id = \"\";";
		}
	}
	
	public static String getPrepareStatements(boolean isPkString, String primaryKeyCol, Map<String,String> columns)
	{
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = columns.keySet().iterator();
		
		int index = 2;
		if(!isPkString) {
			index = 1;
		}
		while(iter.hasNext()){
			String colName = iter.next();
			String tableDataType = columns.get(colName);
			String className = getClassNameForColumnType(tableDataType);
			String propertyName = getPascalCaseName(colName);		
			if(colName.equals("created_date")){
				builder.append("\t\t\tpreparedStatement.setLong("+index+", bean.getCreatedDate()!=null ? bean.getCreatedDate() : getCurrentTimeAsLong());");	
				builder.append(System.getProperty("line.separator"));
				index++;				
			} 
			else if(colName.equals("modified_date")){
				builder.append("\t\t\tif(bean.getModifiedDate()!=null) {"+System.getProperty("line.separator"));
				builder.append("\t\t\t\tpreparedStatement.setLong("+index+", bean.getModifiedDate());"+System.getProperty("line.separator"));
				builder.append("\t\t\t} else {"+System.getProperty("line.separator"));
				builder.append("\t\t\t\tpreparedStatement.setLong("+index+", bean.getCreatedDate()!=null ? bean.getCreatedDate() : getCurrentTimeAsLong());"+System.getProperty("line.separator"));
				builder.append("\t\t\t}"+System.getProperty("line.separator"));		
				index++;
			}
			else if(!colName.equals(primaryKeyCol))
			{
				if(tableDataType.equals("bit") || tableDataType.equals("boolean") ) {
					String block = getNullablePreparedStatementBlock("Types.BOOLEAN", propertyName, className, index);
					builder.append(block);
				} else if(tableDataType.equals("smallint") || tableDataType.equals("integer")) {
					String block = getNullablePreparedStatementBlock("Types.INTEGER", propertyName, "Int", index);
					builder.append(block);
				} else if(tableDataType.equals("bigint")) {
					String block = getNullablePreparedStatementBlock("Types.BIGINT", propertyName, className, index);
					builder.append(block);
				} else if(tableDataType.equals("json") || tableDataType.equals("jsonb")) {
					builder.append("\t\t\tpreparedStatement.setObject("+index+", bean.get"+ propertyName +"(), Types.OTHER);"+System.getProperty("line.separator"));
				} else {
					builder.append("\t\t\tpreparedStatement.set"+className+"("+index+", bean.get"+ propertyName +"());"+System.getProperty("line.separator"));	
				}
				index++;
			}
		}	
		
		return builder.toString();
	}
	
	private static String getNullablePreparedStatementBlock(String jdbcType, String propertyName, String className, int index){
		StringBuilder builder = new StringBuilder();
		builder.append("\t\t\tif(bean.get"+ propertyName +"()!=null) {"+System.getProperty("line.separator")); 
		builder.append("\t\t\t\tpreparedStatement.set"+className+"("+index+", bean.get"+ propertyName +"());"+System.getProperty("line.separator"));							
		builder.append(System.getProperty("line.separator"));
		builder.append("\t\t\t} else {"+System.getProperty("line.separator"));
		builder.append("\t\t\t\tpreparedStatement.setNull("+index+", "+jdbcType+");"+System.getProperty("line.separator"));
		builder.append("\t\t\t}"+System.getProperty("line.separator"));			
		return builder.toString();
	}
	
	public static String getPascalCaseName(String text) 
	{
		StringBuilder builder = new StringBuilder();
		StringTokenizer st2 = new StringTokenizer(text, "_");
		 
		while (st2.hasMoreElements()) {
			String term = (String)st2.nextElement();
			if(!term.isEmpty()) {
				String capTerm = term.substring(0, 1).toUpperCase() + term.substring(1);
				builder.append(capTerm);
			}
		}
		return builder.toString();
	}
	
	public static String getCamelCaseName(String text) 
	{
		StringBuilder builder = new StringBuilder();
		StringTokenizer st2 = new StringTokenizer(text, "_");
		int count = 0;
		while (st2.hasMoreElements()) {
			String term = (String)st2.nextElement();
			if(!term.isEmpty()) {
				String capTerm = term;
				if(count>0) {
					capTerm = term.substring(0, 1).toUpperCase() + term.substring(1);
				}
				builder.append(capTerm);
				count++;
			}
		}
		return builder.toString();
	}	
	
	public static String getClassNameForColumnType(String colType)
	{
		if(colType.startsWith("character")||colType.equals("text")||colType.equals("xml")||colType.equals("json")||colType.equals("jsonb")){
			return "String";
		} else if(colType.equals("smallint")||colType.equals("integer")){
			return "Integer";
		} else if(colType.equals("bigint")){
			return "Long";
		} else if(colType.equals("real")||colType.equals("numeric")||colType.equals("double precision")||colType.equals("money")){
			return "Double";
		} else if(colType.equals("boolean")||colType.equals("bit")){
			return "Boolean";
		} else if(colType.equals("date")){
			return "Date";
		} else if(colType.startsWith("timestamp")){
			return "Timestamp";
		} else {
			throw new IllegalArgumentException("Unknown column type, type name: "+colType);
		}
	}
}
