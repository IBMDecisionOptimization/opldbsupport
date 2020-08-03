# opldbsupport
Support database and Excel for OPL from Java

The code provided here implements support for reading data from and writing data to databases or Excel sheets.

In the examples below we use `"opldbsupport.js"` and `"opldbsupport.jar"`. The former refers to the [opldbsupport.js](opldbsupport.js) in this repository, the latter refers to the [opldbsupport.jar](lib/opldbsupport.jar) file in this repository. In your `.dat` files you may have to adjust the paths to these files so that OPL can find them.

## Limitations

- The code does not check whether too much data is provided. For example, if you fill an array of integers then the code will read the first element in each row of the query result or Excel range. If any row has more than one element then the remaining elements will be silently ignored.

## Databases

In order to add support for a particular database you need a JDBC driver for this database. With this, you can add the following to the top of your `.dat` file:
```
prepare {
  includeScript("opldbsupport.js");
  OPLRegisterJDBC("MyDB", "/path/to/opldbsupport.jar");
  OPLImportJAR("driver.jar");
}
```
After this you can use the following statements in your `.dat` file:
```
MyDBConnection conn("connection string", "extra");
input from MyDBRead(conn, "sql query");
output to MyDBPublish(conn, "sql update");
```
Here
- `connection string` is the JDBC connection string required for your database
- `sql query` is an SQL query statement that must produce data for the element `input`. The result of this query is read row-by-row in order to fill the data element.
- `sql update` is an SQL update or insert statement that stores the value of `output` into the database. If `output` is some sort of collection then the statement is executed for each element in the collection.
- `extra` is semi-colon separated list of arbitrary SQL statements. This may be the empty string. These statements are executed before the *first* publish statement for the connection. If there is no publish statement for the connection then they will not be executed. With these extra statements you can for example prepare tables for output.

### Limitations

- All SQL is directly forwarded to the JDBC driver. So only the syntax supported by the respective driver is supported.
- When reading a tuple, the columns in a row are read left-to-right to fill the fields in the tuple in order of their definition.
- When querying tuples, it is possible to use the `AS fieldname` clause in a select statement to directly assign query output to field names. This way it is possible to read a tuple only partially.
- It is not possible to partially write a tuple. The update statement for a tuple must always have a parameter for each tuple field. If you need to write only parts of a tuple then define and construct a new tuple with only the selected fields in postprocessing.

## Excel

Support for Excel is very similar to databases. Instead of a JDBC driver for your database you need the [APache POI jars](https://poi.apache.org). Then you add the following code to your `.dat` file:
```
prepare {
  includeScript("opldbsupport.js");
  OPLRegisterExcel("Excel", "/path/to/opldbsupport.jar", "/path/to/Apache/POI/installation");
}
```
After this you can use the following statements in your `.dat` file:
```
ExcelConnection conn("/path/to/workbook", "");
input from ExcelRead(conn, "range");
output to ExcelPublish(conn, "range");
```
where `range` is an Excel range like "A1:C3".

Note that for output a range can also be specified as "A1:*", i.e., with the wildcard character `*` as second argument of the range. In this case the code will use the first cell reference (A1 in this case) and fill the rectangular area anchored at this position with the data from the OPL element. This way you don't have to specify the exact size of output tables but can use the size that is implied by the OPL element.

### Limitations

- The code was tested with Apache POI version 4.1.2. Things may not work if you use a different version.
- All I/O is done through Apache POI, so the code only supports the Excel files that this supports.
- Ranges are always processed in row-major form. However, it is possible to append `^T` to a range which causes the range to be transposed before processing.
- The code will not create non-existing sheets for output.
- If the file does not exist for an output operation, then the newly created file will be of XLSX format, no matter what the name of the file is.