// Version 1.0
using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Data;
using System.Xml.Serialization;

namespace GlobalCaptureSQL
{
    public class GlobalCaptureSQL
    {
        //The Assembly that is referenced from the "Call Assembly" Workflow Node must have a method called "RunCallAssembly."
        //The method must accept and return a Dictionary<string, string> Object.
        //The Dictionary Key is the Workflow Property ID and the Value is the Workflow Property Value.
        public Dictionary<string, string> RunCallAssembly(Dictionary<string, string> Input)
        {
            Dictionary<string, string> Output = new Dictionary<string, string>();
            try
            {

                // When calling DLL from GlobalAction, the mappingfile variable's value is C:\GetSmart\PropertyMapping.xml
                string mappingfile = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetCallingAssembly().Location), "PropertyMapping.xml");
                // Check if ProperyMapping.xml exists
                if (System.IO.File.Exists(mappingfile))
                {
                    PropertyMapping propertyMap = new PropertyMapping();
                    propertyMap = propertyMap.Deserialize(mappingfile);
                    // Read values from PropertyMapping.xml and apply to variables
                    string connStrProp = propertyMap.ConnectionString;
                    var connStr = @Input[connStrProp];
                    string qryProp = propertyMap.SqlStatement;
                    var qry = Input[qryProp];
                    string firstReturnProp = propertyMap.FirstReturnProperty;
                    // Return values (to be used for Sql Log text file)
                    var returnValues = new List<string>();
                    // Sql Log file info
                    string logPath = propertyMap.LogOutput;
                    string logFilename = propertyMap.LogFilename;

                    //Check the first six characters of data in Property 2 and covert to uppercase for comparison.
                    //If the SQL query UPDATE/INSERT/DELETE statement, ExecuteNonQuery. Else, skip this section and run SqlDataReader
                    if (qry.Substring(0, 6).ToUpper() == "UPDATE" || qry.Substring(0, 6).ToUpper() == "INSERT" || qry.Substring(0, 6).ToUpper() == "DELETE")
                    {
                        SqlConnection connection = new SqlConnection(connStr);
                        SqlCommand command = new SqlCommand();
                        Int32 rowsAffected;

                        String sqlQuery = qry;

                        command.CommandText = sqlQuery;
                        command.Connection = connection;

                        connection.Open();

                        rowsAffected = command.ExecuteNonQuery();

                        connection.Close();

                        SQL_Log(logPath, logFilename, connStr, sqlQuery, rowsAffected);

                        //return Output;

                    }
                    else

                    {
                        //For each Workflow Property Value that we have in our Input, perform some sort of processing with that data.
                        //In this example, we are taking a SQL Connection string and a SQL query, then running the SQL query to get a value.
                        /***If running stored procedure, ensure there is a return property (Property 3) in GlobalAction***/
                        // (Possible return value may not be necessary)
                        SqlConnection connection = new SqlConnection();
                        SqlCommand command = new SqlCommand();

                        connection.ConnectionString = connStr;

                        String sqlQuery = qry;

                        command.CommandText = sqlQuery;


                        if (connection.ConnectionString != String.Empty && command.CommandText != String.Empty)
                        {
                            connection.Open();
                            command.Connection = connection;
                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                //If any data is read, continue. Else, close the connection.
                                if (reader.Read())
                                {
                                    //Count the number of returned fields from the SQL query and iterate through the loop
                                    //until all the data is added to Output.
                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        //Starting at Property specified for the first return value, increment by 1 for each loop iteration
                                        /****The number of return properties in GlobalAction depends on the number of returned columns****/
                                        /****Subtracting 2 from the Starting Property number will indicate which returned column number will populate the Property****/
                                        //var j = i + firstReturnProp;
                                        //var k = j.ToString();
                                        var k = (i + Convert.ToInt32(firstReturnProp)).ToString();

                                        if (reader[i] != null)
                                        {
                                            //The result that is returned from the SQL query is added to our return Dictionary object as the Property 3,...,Property n Value.
                                            Output.Add(k, reader[i].ToString());
                                            // Add values to returnValues list to be used in Sql Log text file
                                            returnValues.Add(reader[i].ToString());
                                        }
                                    }

                                }

                            }
                            connection.Close();

                            SQL_Log(logPath, logFilename, connStr, sqlQuery, firstReturnProp, returnValues);

                        }

                    }

                }
                else
                {
                    // If PropertyMapping.xml is not in the same directory as this console application, throw an expection.
                    throw new System.ArgumentException("Add PropertyMapping.xml to the same directory as GlobalCaptureSQL.dll", "PropertyMapping.xml not found.");
                }
            }
            catch (Exception ex)
            {
                // Build error file root and full path
                string filePathRoot = @"C:\GetSmart\DLL_Error";
                string filePath = filePathRoot + "\\Error.txt";
                // Create error file directory if it does not exist.
                System.IO.Directory.CreateDirectory(filePathRoot);

                // Creat and write to error text file
                using (StreamWriter writer = new StreamWriter(filePath, true))
                {
                    writer.WriteLine("Message :" + ex.Message + "<br/>" + Environment.NewLine + "StackTrace :" + ex.StackTrace +
                       "" + Environment.NewLine + Environment.NewLine + "Date :" + DateTime.Now.ToString());
                    writer.WriteLine(Environment.NewLine + "-----------------------------------------------------------------------------" + Environment.NewLine);
                }
            }

            //Finally, return our Output Dictionary Object that will be used set the new Values of each Workflow Property.
            //It is only necessary to return the Property ID's and Values of the Properties that are updated.
            return Output;
        }

        // Sql Log output for ExecuteNonQuery
        public static void SQL_Log(String LogPath, String LogFilename, String ConnStr, String Qry, int RowsAffected)
        {

            string filePathRoot = LogPath;
            string filePath = filePathRoot + LogFilename;
            // Create error file directory if it does not exist.
            System.IO.Directory.CreateDirectory(filePathRoot);

            // Creat and write to error text file
            using (StreamWriter writer = new StreamWriter(filePath, true))
            {
                writer.WriteLine("Date................" + DateTime.Now.ToString() + Environment.NewLine +
                                 "Connection String..." + ConnStr + Environment.NewLine +
                                 "Query..............." + Qry + Environment.NewLine +
                                 "Rows Affected......." + RowsAffected + Environment.NewLine +
                                 "-----------------------------------------------------------------------------" + Environment.NewLine);
            }
        }

        // Sql Log output for SqlDataReader
        public static void SQL_Log(String LogPath, String LogFilename, String ConnStr, String Qry, String FirstReturnProp, List<string> ReturnValues)
        {

            string filePathRoot = LogPath;
            string filePath = filePathRoot + LogFilename;
            // Create error file directory if it does not exist.
            System.IO.Directory.CreateDirectory(filePathRoot);

            // Creat and write to error text file
            using (StreamWriter writer = new StreamWriter(filePath, true))
            {
                var counter = 1;
                writer.WriteLine("Date................" + DateTime.Now.ToString() + Environment.NewLine +
                                 "Connection String..." + ConnStr + Environment.NewLine +
                                 "Query..............." + Qry + Environment.NewLine +
                                 "FirstReturnPropNum.." + FirstReturnProp);
                foreach (var item in ReturnValues)
                {
                    writer.WriteLine("Return Value " + counter + "......" + item);
                    counter++;
                }
                writer.WriteLine(Environment.NewLine + "-----------------------------------------------------------------------------" + Environment.NewLine);
            }
        }
    }
}

// <summary>
// create an object that will match the xml file
// </summary>
[XmlRoot("PropertyMapping")]
public class PropertyMapping
{

    public string ConnectionString { get; set; }
    public string SqlStatement { get; set; }
    public string FirstReturnProperty { get; set; }
    public string LogOutput { get; set; }
    public string LogFilename { get; set; }


    public void Serialize(String file, PropertyMapping propertyMap)
    {
        System.Xml.Serialization.XmlSerializer xs
           = new System.Xml.Serialization.XmlSerializer(propertyMap.GetType());
        StreamWriter writer = System.IO.File.CreateText(file);
        xs.Serialize(writer, propertyMap);
        writer.Flush();
        writer.Close();
        writer.Dispose();
    }

    public PropertyMapping Deserialize(string file)
    {
        System.Xml.Serialization.XmlSerializer xs
           = new System.Xml.Serialization.XmlSerializer(
              typeof(PropertyMapping));
        StreamReader reader = System.IO.File.OpenText(file);
        PropertyMapping propertyMap = (PropertyMapping)xs.Deserialize(reader);
        reader.Close();
        reader.Dispose();
        return propertyMap;
    }
}

