using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace CMDB_SMAX_Integration
{
    partial class Credentials
    {
        public string username
        {
            get; set;
        }
        public string password
        {
            get; set;
        }
        public string clientContext
        {
            get; set;
        }
    }

    partial class QueryName
    {
        public string Name
        {
            get; set;
        }

    }

    partial class JList
    {
        public string TableName
        {
            get; set;
        }
        public JObject JsonObject
        {
            get; set;
        }
    }

    public class DBHelper
    {
        private SqlDataAdapter myAdapter;
        private SqlConnection conn;

        /// <constructor>
        /// Initialise Connection
        /// </constructor>
        public DBHelper()
        {
            myAdapter = new SqlDataAdapter();
            String strConnection = GetConnectionString();
            conn = new SqlConnection(strConnection);
        }

        public string GetConnectionString()
        {
            string dbName = ConfigurationManager.AppSettings.Get("Databasename");
            string dbServer = ConfigurationManager.AppSettings.Get("DatabaseServer");
            String strConnection = "Integrated Security=False;User ID=etlunidashbrd;Password=zxQaL7#dbo;Initial Catalog=" + dbName + "; Data Source =" + dbServer;
            return strConnection;
        }

        /// <method>
        /// Open Database Connection if Closed or Broken
        /// </method>
        private SqlConnection openConnection()
        {
            if (conn.State == ConnectionState.Closed || conn.State ==
                        ConnectionState.Broken)
            {
                conn.Open();
            }
            return conn;
        }

        /// <method>
        /// Select Query
        /// </method>
        public DataTable executeSelectQuery(String _query, SqlParameter[] sqlParameter)
        {
            SqlCommand myCommand = new SqlCommand();
            DataTable dataTable = new DataTable();
            dataTable = null;
            DataSet ds = new DataSet();
            try
            {
                myCommand.Connection = openConnection();
                myCommand.CommandText = _query;
                if (sqlParameter != null)
                    myCommand.Parameters.AddRange(sqlParameter);
                myCommand.ExecuteNonQuery();
                myCommand.CommandTimeout = 0;
                myAdapter.SelectCommand = myCommand;
                myAdapter.Fill(ds);
                dataTable = ds.Tables[0];
            }
            catch (SqlException e)
            {
                Console.Write("Error - Connection.executeSelectQuery - Query: " + _query + " \nException: " + e.StackTrace.ToString());

                return null;
            }
            finally
            {

            }
            return dataTable;
        }

        /// <method>
        /// Insert Query
        /// </method>
        public bool executeInsertQuery(String _query, SqlParameter[] sqlParameter)
        {
            SqlCommand myCommand = new SqlCommand();
            try
            {
                myCommand.Connection = openConnection();
                myCommand.CommandText = _query;
                myCommand.CommandTimeout = 0;
                myCommand.Parameters.AddRange(sqlParameter);
                myAdapter.InsertCommand = myCommand;
                myCommand.ExecuteNonQuery();
            }
            catch (SqlException e)
            {
                Console.Write("Error - Connection.executeInsertQuery - Query: " + _query + " \nException: \n" + e.StackTrace.ToString());
                return false;
            }
            finally
            {
            }
            return true;
        }

        /// <method>
        /// Update Query
        /// </method>
        public bool executeUpdateQuery(String _query, SqlParameter[] sqlParameter)
        {
            SqlCommand myCommand = new SqlCommand();
            try
            {
                myCommand.Connection = openConnection();
                myCommand.CommandText = _query;
                myCommand.Parameters.AddRange(sqlParameter);
                myAdapter.UpdateCommand = myCommand;
                myCommand.ExecuteNonQuery();
            }
            catch (SqlException e)
            {
                Console.Write("Error - Connection.executeUpdateQuery - Query:  " + _query + " \nException: " + e.StackTrace.ToString());
                return false;
            }
            finally
            {
            }
            return true;
        }

        public void BulkInsert(string connString, string json, int resourceType, string tableName, string primaryColumnName, DataTable dataTable)
        {
            //DataTable dataTable = Tabulate(json, resourceType, tableName, primaryColumnName);
            if (dataTable.Rows.Count <= 0)
                return;
            using (SqlBulkCopy sqlBulk = new SqlBulkCopy(connString))
            {
                sqlBulk.DestinationTableName = tableName;
                sqlBulk.BulkCopyTimeout = 0;
                try
                {
                    sqlBulk.WriteToServer(dataTable);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }

        private DataTable JsonToDataTable()
        {
            string json = "";
            DataTable dt = (DataTable)JsonConvert.DeserializeObject(json, (typeof(DataTable)));
            return dt;
        }

        public bool HasProperties(string json, int cmdbType, string tableName, string primaryColumnName)
        {
            if (string.IsNullOrEmpty(json))
                return false;
            if (!json.Contains("ucmdbId"))
                return false;
            var jsonLinq = JObject.Parse(json);
            string test = "";
            if (tableName.ToLower() == "installedsoftware")
                test = "installedsoftware";
            // Find the first array using Linq
            var srcArray = jsonLinq.Descendants().Where(d => d is JArray).First();

            foreach (JObject row in srcArray.Children<JObject>())
            {
                string resourceType = tableName;
                bool tableFound = false;
                if (row.Properties().Any(x => x.Name == "label" && x.Value.ToString().ToLower() == "computer"))
                    tableFound = true;
                if (row.Properties().Any(x => x.Name == "label" && x.Value.ToString().ToLower() == "installedsoftware"))
                    tableFound = true;
                if (tableFound)
                {
                    foreach (JProperty column in row.Properties())
                    {
                        // Only include JValue types
                        if (column.Value is JValue)
                        {

                        }
                        else if (column.Value is JContainer)
                        {
                            if (((JObject)column.Value).Properties().Count() < 2)
                            {
                                return false;
                            }
                        }
                    }

                    //if (row.Properties().Count() < 2)
                    //    return false;
                    //else
                    //    return true;
                }
            }
            return true;
        }


        public DataSet GetProperties(string json, string primaryColumnName)
        {
            DataSet ds = new DataSet();
            if (string.IsNullOrEmpty(json))
                return null;
            if (!json.Contains("ucmdbId"))
                return null;
            var jsonLinq = JObject.Parse(json);
            // Find the first array using Linq

            //JToken srcArray = null;
            var srcArray = jsonLinq.Descendants().Where(d => d is JArray).First();
            List<JToken> tokenList = jsonLinq.Descendants().Where(d => d is JArray).ToList();
            if (json.Contains("Composition"))
            {
                srcArray = jsonLinq.Descendants().Where(d => d is JArray).Last();
                //if (tokenList.Count > 1)
                //    srcArray = tokenList[1].First();
            }

            var trgArray = new JArray();
            
            List<JList> jObjectList = new List<JList>();
            List<string> tables = new List<string>();
            foreach (JObject row in srcArray.Children<JObject>())
            {
                var cleanRow = new JObject();
                bool tableFound = false;
                string tableName = string.Empty;
                if (row.Properties().Any(x => x.Name == "label" && x.Value.ToString().ToLower() == "computer"))
                    tableFound = true;
                if (row.Properties().Any(x => x.Name == "label" && x.Value.ToString().ToLower() == "installedsoftware"))
                    tableFound = true;
                if (row.Properties().Any(x => x.Name == "label" && x.Value.ToString().ToLower() == "composition"))
                {
                    tableName = "Composition";
                    tableFound = true;
                }
                if (tableFound)
                {
                    foreach (JProperty column in row.Properties())
                    {
                        if (column.Name == "label" && column.Value != null)
                        {
                            tableName = column.Value.ToString();
                            if (!tables.Contains(tableName))
                                tables.Add(tableName);
                        }
                        // Only include JValue types
                        if (column.Value is JValue)
                        {
                            if (column.Name == primaryColumnName)
                                cleanRow.Add(column.Name, column.Value);
                            if (tableName == "Composition")
                            {
                                if (column.Name.Contains("end1Id") || column.Name.Contains("end2Id"))
                                    cleanRow.Add(column.Name, column.Value);
                            }
                        }
                        else if (column.Value is JContainer)
                        {
                            if (((JObject)column.Value).Properties().Count() < 2 && tableName != "Composition")
                                return null;
                            foreach (JProperty propertyColumn in ((JObject)column.Value).Properties())
                            {
                                if (propertyColumn.Value is JValue)
                                    cleanRow.Add(propertyColumn.Name, propertyColumn.Value);
                            }
                        }
                    }
                    trgArray.Add(cleanRow);

                    JList jList = new JList();
                    jList.TableName = tableName;
                    jList.JsonObject = cleanRow;
                    jObjectList.Add(jList);
                }
            }
            if (tables.Count > 0)
            {
                foreach (string table in tables)
                {
                    List<JList> l = jObjectList.Where(x => x.TableName == table).ToList();
                    trgArray = new JArray(l.Select(x => x.JsonObject).ToArray());
                    DataTable dt = JsonConvert.DeserializeObject<DataTable>(trgArray.ToString());
                    dt.TableName = table;
                    ds.Tables.Add(dt);
                }
                return ds;
            }
            return null;
        }
        private DataTable Tabulate(string json, int cmdbType, string tableName, string primaryColumnName)
        {
            var jsonLinq = JObject.Parse(json);
            string test = "";
            if (tableName.ToLower() == "installedsoftware")
                test = "installedsoftware";
            // Find the first array using Linq
            var srcArray = jsonLinq.Descendants().Where(d => d is JArray).First();

            var trgArray = new JArray();
            foreach (JObject row in srcArray.Children<JObject>())
            {
                var cleanRow = new JObject();
                string resourceType = tableName;
                if (row.Properties().Any(x => x.Name == "label" && x.Value.ToString().ToLower() == "installedsoftware"))
                    test = "installedsoftware";
                if (row.Properties().Any(x => x.Name == "label" && x.Value.ToString() == resourceType))
                //if (rowMatch != null)
                {

                    foreach (JProperty column in row.Properties())
                    {
                        // Only include JValue types
                        if (column.Value is JValue)
                        {
                            if (column.Name == primaryColumnName)
                                cleanRow.Add(column.Name, column.Value);
                        }
                        else if (column.Value is JContainer)
                        {
                            foreach (JProperty propertyColumn in ((JObject)column.Value).Properties())
                            {
                                if (propertyColumn.Value is JValue)
                                {
                                    cleanRow.Add(propertyColumn.Name, propertyColumn.Value);
                                }
                            }
                        }
                    }

                    trgArray.Add(cleanRow);
                }
            }

            return JsonConvert.DeserializeObject<DataTable>(trgArray.ToString());
        }

        public async Task<string> GetToken()
        {
            HttpClient hc;
            hc = new HttpClient();
            HttpContent content;
            var credentials = new Credentials();
            credentials.username = ConfigurationManager.AppSettings.Get("LoginID");
            credentials.password = ConfigurationManager.AppSettings.Get("Password");
            credentials.clientContext = ConfigurationManager.AppSettings.Get("ClientContext");
            content = new StringContent(JsonConvert.SerializeObject(credentials), Encoding.UTF8, "application/json");
            Task<System.Net.Http.HttpResponseMessage> trm;
            string base_URI = ConfigurationManager.AppSettings.Get("Base_URI");
            trm = hc.PostAsync(base_URI + "/authenticate/", content);
            System.Net.Http.HttpResponseMessage rm;
            rm = trm.Result;
            string responseContent = await rm.Content.ReadAsStringAsync();
            return responseContent;
        }

        public async Task<string> GetData(dynamic token, string uri, string queryName)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization =
    new AuthenticationHeaderValue("Bearer", token.Value);
            //client.DefaultRequestHeaders.Add("Accept", "application/json");
            //client.DefaultRequestHeaders.Add("Cookie", "LWSSO_COOKIE_KEY= " + token.Value);
            Task<System.Net.Http.HttpResponseMessage> trm;
            HttpContent content;
            if (queryName.Length != 0)
            {
                content = new StringContent(queryName, Encoding.UTF8, "application/json");
                trm = client.PostAsync(uri, content);
            }
            else
            {
                trm = client.GetAsync(uri);
            }
            System.Net.Http.HttpResponseMessage rm = trm.Result;
            return await rm.Content.ReadAsStringAsync();
        }

        public async Task<string> GetData1(string token, string uri)
        {
            string jsonData = string.Empty;
            HttpClient client = new HttpClient();
            try
            {
                //Query_112
                client.DefaultRequestHeaders.Add("Accept", "application/json");
                client.DefaultRequestHeaders.Add("Cookie", "LWSSO_COOKIE_KEY= " + token);
                HttpResponseMessage response = client.GetAsync(uri).Result;
                if (response.IsSuccessStatusCode)
                    jsonData = await response.Content.ReadAsStringAsync();
            }
            catch (Exception ex)
            {
            }
            return jsonData;
        }
    }
}
