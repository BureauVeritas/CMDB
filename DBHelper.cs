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
using System.Net.Mail;
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

        public async void InsertData(string baseURI, int chunk_size, string queryName, dynamic token)
        {
            string uri = baseURI + "/topology" + "?chunkSize=" + chunk_size;
            dynamic data = "";
            string json = "";
            //check for error like smax
            while (!json.Contains("numberOfChunks"))
            {
                json = await GetData(token, uri, queryName);
            }
            data = JObject.Parse(json);

            for (int items = 1; items <= Convert.ToInt32(data.numberOfChunks); items++)
            {
                uri = baseURI + "/topology/result/" + data.queryResultId + "/" + items;
                json = "";
                DataSet ds = null;
                while (ds == null)
                {
                    //check for error like smax
                    json = await GetData(token, uri, "");
                    ds = GetProperties(json, "ucmdbId");
                }
                InsertJsonIntoTable("Computer", ds, json);
                //InsertJsonIntoTable("InstalledSoftware", ds, json);
                //InsertJsonIntoTable("Composition", ds, json);

                //InsertJsonIntoTable("Mapping", ds, json);
            }
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

        public async void SendEmail(string ExceptionMessage)
        {
            try
            {
                MailMessage mail = new MailMessage();
                SmtpClient SmtpServer = new SmtpClient("mta-asi.bureauveritas.com");

                mail.To.Add(ConfigurationManager.AppSettings.Get("ToEmailAddresses"));
                mail.Subject = "CMDB Sync for Devices/Assets was Sucessfull:" + DateTime.Now.ToString();
                mail.Body = ExceptionMessage;
                mail.From = new MailAddress("SMAXDashBoardTool@bureauveritas.com");

                SmtpServer.Port = 25;
                SmtpServer.Credentials = new System.Net.NetworkCredential("s-GsscApps@ASI.bvcorp.corp", "vTpAE8qzxVTpaj3N");
                SmtpServer.EnableSsl = true;

                SmtpServer.Send(mail);
            }
            catch (Exception e)
            {

            }
        }

        public DataTable ExecuteSelectQuery(String _query, SqlParameter[] sqlParameter)
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

        private async Task<string> GetData(dynamic token, string uri, string queryName)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token.Value);
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

        private DataSet GetProperties(string json, string primaryColumnName)
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

        private string GetConnectionString()
        {
            string dbName = ConfigurationManager.AppSettings.Get("Databasename");
            string dbServer = ConfigurationManager.AppSettings.Get("DatabaseServer");
            String strConnection = "Integrated Security=False;User ID=etlunidashbrd;Password=zxQaL7#dbo;Initial Catalog=" + dbName + "; Data Source =" + dbServer;
            return strConnection;
        }

        private SqlConnection openConnection()
        {
            if (conn.State == ConnectionState.Closed || conn.State ==
                        ConnectionState.Broken)
            {
                conn.Open();
            }
            return conn;
        }

        private void InsertJsonIntoTable(string tableName, DataSet ds, string json)
        {
            if (ds.Tables[tableName] != null && ds.Tables[tableName].Rows.Count > 0)
            {
                List<string> columns = GetColumns(tableName);
                DataTable dataTable = new DataTable();
                dataTable = SetColumnsOrder(ds.Tables[tableName], columns);
                BulkInsert(json, 0, tableName, "ucmdbId", ds.Tables[tableName]);
            }
        }

        private void BulkInsert(string json, int resourceType, string tableName, string primaryColumnName, DataTable dataTable)
        {
            string connString = GetConnectionString();
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

        private List<string> GetColumns(string tableName)
        {
            string strConnection = GetConnectionString();
            SqlConnection conn = new SqlConnection(strConnection);
            List<string> columnList = new List<string>();
            DataTable dataTable = new DataTable();
            string cmdText = "Select top 1 * from " + tableName;
            using (SqlCommand cmd = new SqlCommand(cmdText, conn))
            {
                cmd.CommandType = CommandType.Text;
                SqlDataAdapter da = new SqlDataAdapter();
                da.SelectCommand = cmd;
                da.Fill(dataTable);
            }
            if (dataTable.Columns != null && dataTable.Columns.Count > 0)
            {
                foreach (DataColumn column in dataTable.Columns)
                {
                    columnList.Add(column.ColumnName);
                }
            }
            return columnList;
        }

        private DataTable SetColumnsOrder(DataTable table, List<string> columnNames)
        {
            int columnIndex = 0;
            if (columnNames != null && columnNames.Count > 0)
            {
                foreach (var columnName in columnNames)
                {
                    if (table.Columns.Contains(columnName))
                    {
                        table.Columns[columnName].SetOrdinal(columnIndex);
                        columnIndex++;
                    }
                }
            }
            return table;
        }
    }
}
