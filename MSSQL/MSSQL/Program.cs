using System;
using System.Data.SqlClient;
using System.Threading;

// dotnet add package System.Data.SqlClient

class MSSQL
{
    private SqlConnection connection;
    private string connectionString;

    public MSSQL(string serverName, string dbName, string username, string password)
    {
        connectionString = $@"
Data Source={serverName};
Initial Catalog={dbName};
User ID={username};
Password={password}";
        connection = new SqlConnection(connectionString);
    }

    public void OpenConnection()
    {
        if (connection.State != System.Data.ConnectionState.Open)
        {
            connection.Open();
        }
    }

    public void CloseConnection()
    {
        if (connection.State != System.Data.ConnectionState.Closed)
        {
            connection.Close();
        }
    }

    public SqlDataReader ExecuteQuery(string query)
    {
        SqlCommand command = new SqlCommand(query, connection);
        return command.ExecuteReader();
    }

    public SqlDataReader FetchallQuery(string query, Dictionary<string, Type> schema)
    {
        SqlCommand command = new SqlCommand(query, connection);
        SqlDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
            foreach (var column in schema)
            {
                string columnName = column.Key;
                Type columnType = column.Value;

                int columnIndex = reader.GetOrdinal(columnName);
                
                if (columnType == typeof(int))
                {
                    int value = reader.GetInt32(columnIndex);
                    Console.WriteLine($"{columnName}: {value}");
                }
                else if (columnType == typeof(string))
                {
                    string value = reader.GetString(columnIndex);
                    Console.WriteLine($"{columnName}: {value}");
                }
                else if (columnType == typeof(float))
                {
                    float value = reader.GetFloat(columnIndex);
                    Console.WriteLine($"{columnName}: {value}");
                }
                else if (columnType == typeof(DateTime))
                {
                    DateTime value = reader.GetDateTime(columnIndex);
                    Console.WriteLine($"{columnName}: {value}");
                }
                else if (columnType == typeof(bool))
                {
                    bool value = reader.GetBoolean(columnIndex);
                    Console.WriteLine($"{columnName}: {value}");
                }

            }
        }
        return command.ExecuteReader();
    }

}

//class CreateTable
//{
//    static void Main()
//    {
//        MSSQL mssql = new MSSQL("",
//                                "demo",
//                                "hooniegit",
//                                "Dh8835013!");

//        try
//        {
//            mssql.OpenConnection();

//            string query = $@"
//        CREATE TABLE SensorTelemet (
//        ts float,
//        device varchar(20),
//        co float,
//        humidity float,
//        light varchar(5),
//        lpg float,
//        motion varchar(5),
//        smoke float,
//        temp float
//        )";
//            mssql.ExecuteQuery(query);
//            Console.WriteLine("Table Created!");
//        }

//        catch (Exception ex)
//        {
//            Console.WriteLine($"Error: {ex.Message}");
//        }

//        finally
//        {
//            mssql.CloseConnection();
//        }
//    }
//}

//class ReadTable
//{
//    static void Main()
//    {
//        MSSQL mssql = new MSSQL("",
//                                "demo",
//                                "hooniegit",
//                                "Dh8835013!");

//        try
//        {
//            mssql.OpenConnection();

//            string query = $"SELECT * FROM ArduinoSensorValues";

//            SqlDataReader reader = mssql.ExecuteQuery(query);

//            Console.WriteLine("Query Succeed!");

//            while (reader.Read())
//            {
//                int id = reader.GetInt32(0);
//                string columnName = reader.GetString(1);

//                Console.WriteLine($"ID: {id}, ColumnName: {columnName}");
//            }
//        }

//        catch (Exception ex)
//        {
//            Console.WriteLine($"Error: {ex.Message}");
//        }

//        finally
//        {
//            mssql.CloseConnection();
//        }
//    }
//}

class Program
{
    static void Main()
    {
        // server informations
        string serverName = "";
        string dbName = "demo";
        string username = "hooniegit";
        string password = "Dh8835013!";

        // query informations & defaults
        string csvFilePath = "/Users/kimdohoon/git/dataset/kaggle/csv/iot_telemetry_data.csv";
        string tableName = "SensorTelemet";
        string insertQuery = $"INSERT INTO {tableName} VALUES";

        Console.WriteLine("START >>>>>>>>>>");

        try
        {
            bool isFirstRow = true;
            using (StreamReader reader = new StreamReader(csvFilePath))
            {
                while (!reader.EndOfStream)
                {
                    //Thread.Sleep(1000);

                    string[] values = reader.ReadLine().Split(',');
                    Console.WriteLine($"value : {values}");

                    // pass schema row
                    if (isFirstRow)
                    {
                        Console.WriteLine("First Row is Passed."); // test
                        isFirstRow = false;
                        continue;
                    }

                    // append values to insert query
                    insertQuery += $" ({GetFormattedValues(values)}),";
                    Console.WriteLine($"query : {insertQuery}");
                }
            }

            // check insert query
            insertQuery = insertQuery.TrimEnd(',');
            Console.WriteLine(insertQuery); // test
            Console.WriteLine("Data String Created Successfully."); // test
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
        finally
        {
        }

        //create mssql &open
        MSSQL mssql = new MSSQL(serverName, dbName, username, password);
        mssql.OpenConnection();

        try
        {
            // execute query
            mssql.ExecuteQuery(insertQuery);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
        finally
        {
            // close connections
            mssql.CloseConnection();
        }

    }

    // remove Ns to DATETIME data
    private static string GetFormattedValues(string[] values)
    {
        List<string> formattedValues = new List<string>();

        foreach (var value in values)
        {
            if (value.StartsWith("\"") && value.EndsWith("\"") || value.StartsWith("'") && value.EndsWith("'"))
            {
                string parsed_value = value.Substring(1, value.Length - 2);

                if (DateTime.TryParse(value, out _))
                {
                    formattedValues.Add($"'{parsed_value}'");
                }
                else
                {
                    formattedValues.Add($"N'{parsed_value}'");
                }
            }
            else
            {
                if (DateTime.TryParse(value, out _))
                {
                    formattedValues.Add($"'{value}'");
                }
                else
                {
                    formattedValues.Add($"N'{value}'");
                }
            }
        }

        return string.Join(", ", formattedValues);
    }
}
