# jersey-api-generator
A code generator for building Jersey 2.0 REST web services, based on PostgreSQL tables.

In a nutshell it's a Java class that reads the database table's metadata and processes them using text templates to generate the desired output. By default it comes with templates for generating: Beans, DAOs, Jersey 2.0 endpoints.

The provided templates generate CRUD like Jersey 2.0 endpoints for each table. Templates can be found as .temp files in the src/resources folder. They can be modified to generate the language/output of your preference based on the database table's metadata.

A database schema provider could be implemented for the database of your choice.

## Installation 
You can compile it by running the following maven command:

```
mvn clean compile assembly:single
```
Then invoke the main class passing the required database connection arguments:

```
java -cp target/jersey-api-generator-1.0-jar-with-dependencies.jar com.orangeloops.apigen.Generator mydbhost dbName dbuser dbPass {target tableName}
```

The table names to include in the generation can either be passed in the command line, or if it's more than one table, you can edit the src/resources/TablesToInclude.txt file to include all the table names separated by line breaks.

Once the generation is completed, you will find the generated files in the output folder, and copy them to your project, or just edit the Generator class to modify the output folders to generate directly to your target project folders.
