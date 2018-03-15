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
