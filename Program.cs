
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Newtonsoft.Json;
using System.Xml.Linq;
using Microsoft.Spark.Sql.Streaming;

namespace MySparkApp
{
class Program
{
    static void Main(string[] args)
    {
        string xmlFilePath = "orders.xml";
        
        var spark = SparkSession.Builder()
            .AppName("SparkXMLExample")
            .Config("spark.sql.hive.convertMetastoreParquet","false")
            .Config("spark.serializer","org.apache.spark.serializer.KryoSerializer").GetOrCreate();
           

Console.WriteLine("\n\n>>>>>>>>>>>>>spark session created>>>>>>>>>\n\n");

        DataFrame df = spark.Read()
            .Format("com.databricks.spark.xml")
            .Option("spark.jars", "spark-xml_2.12-0.12.0.jar")  // Specify the format as spark-xml
            .Option("rowTag", "Order")  // Specify the XML row tag to read
            .Load(xmlFilePath);
        

        // Process DataFrame as needed
        df.Show();

        // df.Select("OrderId","OrderDate")
        //     .Write()
        //     .Mode(SaveMode.Overwrite)  // Specify the mode (Overwrite, Append, etc.)
        //     .Parquet("dim_order.parquet");

        // df.Select("CustomerId","CustomerName","Phone","Address","City","Region","PostalCode","Country")
        //     .Write()
        //     .Mode(SaveMode.Overwrite)  // Specify the mode (Overwrite, Append, etc.)
        //     .Parquet("dim_customer.parquet");

        // df.Select("ProductID","ProductName","ProductCategory","UnitPrice")
        //     .Write()
        //     .Mode(SaveMode.Overwrite)  // Specify the mode (Overwrite, Append, etc.)
        //     .Parquet("dim_product.parquet");
                    
        // df.Select("EmployeeId","EmployeeName","DepartmentName")
        //     .Write()
        //     .Mode(SaveMode.Overwrite)  // Specify the mode (Overwrite, Append, etc.)
        //     .Parquet("dim_employee.parquet");

        // df.Select("OrderId","CustomerId","EmployeeId","ProductID","EmployeeName")
        //     .Write()
        //     .Mode(SaveMode.Overwrite)  // Specify the mode (Overwrite, Append, etc.)
        //     .Parquet("fact_orders.parquet");


        Console.WriteLine("\n\n>>>>>>>>>>>>>Writing to Hudi table>>>>>>>>>\n\n");

        df.Select("OrderId","OrderDate")
            .Write()
            .Format("org.apache.hudi")
            .Option("hoodie.table.name","dim_order")
            .Option("hoodie.datasource.write.table.name","dim_order")
            .Option("hoodie.datasource.write.recordkey.field", "OrderId")
            .Option("hoodie.datasource.write.precombine.field", "OrderDate")
            .Option("hoodie.datasource.write.operation", "upsert")
            .Option("hoodie.datasource.write.storage.type", "COPY_ON_WRITE")
            .Mode("overwrite")
            .Save("file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_order");

        df.Select("CustomerId","CustomerName","Phone","Address","City","Region","PostalCode","Country")
            .Write()
            .Format("org.apache.hudi")
            .Option("hoodie.table.name","dim_customer")
            .Option("hoodie.datasource.write.table.name","dim_customer")
            .Option("hoodie.datasource.write.recordkey.field", "CustomerId")
            .Option("hoodie.datasource.write.precombine.field", "Country")
            .Option("hoodie.datasource.write.operation", "upsert")
            .Option("hoodie.datasource.write.storage.type", "COPY_ON_WRITE")
            .Mode("overwrite")
            .Save("file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_customer");

        df.Select("ProductID","ProductName","ProductCategory","UnitPrice")
            .Write()
            .Format("org.apache.hudi")
            .Option("hoodie.table.name","dim_product")
            .Option("hoodie.datasource.write.table.name","dim_product")
            .Option("hoodie.datasource.write.recordkey.field", "ProductID")
            .Option("hoodie.datasource.write.precombine.field", "ProductCategory")
            .Option("hoodie.datasource.write.operation", "upsert")
            .Option("hoodie.datasource.write.storage.type", "COPY_ON_WRITE")
            .Mode("overwrite")
            .Save("file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_product");

        df.Select("EmployeeId","EmployeeName","DepartmentName")
            .Write()
            .Format("org.apache.hudi")
            .Option("hoodie.table.name","dim_employee")
            .Option("hoodie.datasource.write.table.name","dim_employee")
            .Option("hoodie.datasource.write.recordkey.field", "EmployeeId")
            .Option("hoodie.datasource.write.precombine.field", "DepartmentName")
            .Option("hoodie.datasource.write.operation", "upsert")
            .Option("hoodie.datasource.write.storage.type", "COPY_ON_WRITE")
            .Mode("overwrite")
            .Save("file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_employee");

        df.Select("OrderId","CustomerId","EmployeeId","ProductID","EmployeeName")
            .Write()
            .Format("org.apache.hudi")
            .Option("hoodie.table.name","fact_orders")
            .Option("hoodie.datasource.write.table.name","fact_orders")
            .Option("hoodie.datasource.write.recordkey.field", "OrderId,CustomerId")
            .Option("hoodie.datasource.write.precombine.field", "CustomerId")
            .Option("hoodie.datasource.write.operation", "upsert")
            .Option("hoodie.datasource.write.storage.type", "COPY_ON_WRITE")
            .Mode("overwrite")
            .Save("file:///D:/SparkAppDemo/sparkhudi-warehouse/fact_orders");

       
        Console.WriteLine("\n\n>>>>>>>>>>>>>Successfully Write to Hudi table>>>>>>>>>\n\n");
        
        // Console.WriteLine("\n\n>>>>>>>>>>>>>Reading from Hudi table>>>>>>>>>\n\n");

        // DataFrame dim_order_df = spark.Read()
        //     .Format("org.apache.hudi")
        //     .Option("hoodie.datasource.table.name", "dim_order")
        //     .Option("hoodie.datasource.base.path", "file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_order")
        //     .Load("file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_order");

        // DataFrame dim_customer_df = spark.Read()
        //         .Format("org.apache.hudi")
        //         .Option("hoodie.datasource.table.name", "dim_customer")
        //         .Option("hoodie.datasource.base.path", "file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_customer")
        //         .Load("file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_customer");

        // DataFrame dim_product_df = spark.Read()
        //         .Format("org.apache.hudi")
        //         .Option("hoodie.datasource.table.name", "dim_product")
        //         .Option("hoodie.datasource.base.path", "file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_product")
        //         .Load("file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_product");

        // DataFrame dim_employee_df = spark.Read()
        //         .Format("org.apache.hudi")
        //         .Option("hoodie.datasource.table.name", "dim_employee")
        //         .Option("hoodie.datasource.base.path", "ffile:///D:/SparkAppDemo/sparkhudi-warehouse/dim_employee")
        //         .Load("file:///D:/SparkAppDemo/sparkhudi-warehouse/dim_employee");

        // DataFrame fact_orders_df = spark.Read()
        //         .Format("org.apache.hudi")
        //         .Option("hoodie.datasource.table.name", "fact_orders")
        //         .Option("hoodie.datasource.base.path", "file:///D:/SparkAppDemo/sparkhudi-warehouse/fact_orders")
        //         .Load("file:///D:/SparkAppDemo/sparkhudi-warehouse/fact_orders");

        // Console.WriteLine("\n\n>>>>>>>>>>>>>Display dim_order_df Hudi table>>>>>>>>>\n\n");

        // dim_order_df.Show();

        // Console.WriteLine("\n\n>>>>>>>>>>>>>Display dim_customer_df Hudi table>>>>>>>>>\n\n");

        // dim_customer_df.Show();

        // Console.WriteLine("\n\n>>>>>>>>>>>>>Display dim_product_df Hudi table>>>>>>>>>\n\n");

        // dim_product_df.Show();

        // Console.WriteLine("\n\n>>>>>>>>>>>>>Display dim_employee_df Hudi table>>>>>>>>>\n\n");

        // dim_employee_df.Show();

        // Console.WriteLine("\n\n>>>>>>>>>>>>>Display fact_orders_df Hudi table>>>>>>>>>\n\n");

        // fact_orders_df.Show();

        spark.Stop();
    }
}
}

