# CSV To Parquet Converter and Parquet Reader

## Getting Started

The recommend way of use is to build jar using maven plugin and run it with parameters using java command.

* Build project with maven assembly plugin:
```
mvn clean compile assembly:single
```
* Run jar with 4 parameters using java:
```
java -jar pathToJarFile.jar parameter1 parameter2 parameter3 parameter4

pathToJarFile.jar: Path to generated jar file
parameter1: Path to avro schema(avsc file)
parameter2: Path to csv file
parameter3: Path to parquet file
parameter4: Number of maximum rows will be displayed after parquet file generation
```

## ParquetUtils methods

public static void parseSchema

    /**
     * Returns avro schema
     * @param schemaPath path to avsc file
     * @return Schema
     */

public static void writeToParquet
     
    /**
     * Convert csv to parquet and write to file
     * @param schema avro schema
     * @param csvPath path to csv we are going to convert
     * @param parquetPath path to parquet file that will be generated from csv
     */
     
public static void readParquetFile
     
    /**
     * Printing parquet contents
     * @param parquetFilePath path to parquet file for reading
     * @param maxRows number of maximum rows will be displayed
     */
     
## Running the tests

* Please open the following class file:
```
src/test/java/com/app/ParquetUtilsTest.java
```
* Run `writeToParquet()` method to test CSV to parquet converting
* Run `readParquetFile()` method to test parquet reading and printing