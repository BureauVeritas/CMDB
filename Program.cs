using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Net.Mail;
using System.Threading.Tasks;

namespace CMDB_SMAX_Integration
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                DBHelper helper = new DBHelper();
                string query = "dbo.truncate_ComputerSoftware";
                DataTable dtRun = new DataTable();
                dtRun = helper.executeSelectQuery(query, null);

                int chunk_size = Convert.ToInt32(ConfigurationManager.AppSettings.Get("CHUNK_SIZE"));
                string uri = ConfigurationManager.AppSettings.Get("Base_URI") + "/topology" + "?chunkSize=" + chunk_size;

                //get authentication token
                var token = "";
                while (!token.Contains("token"))
                {
                    token = await helper.GetToken();
                }
                dynamic data = "";
                data = JObject.Parse(token);
                dynamic tokenfinal = data.token;
                string queryNames = ConfigurationManager.AppSettings.Get("QueryName");
                string[] queries = queryNames.Split(',');

                if (queries.Length > 0)
                {
                    //Stopwatch sp = new Stopwatch();
                    //sp.Start();
                    //InsertData(uri, queries[0], tokenfinal);
                    //Uncomment for loop below if needs to debug and comment Parallel ForEaach
                    //foreach (string queryName in queries)
                    //{
                    //    InsertData(uri, queryName, tokenfinal);
                    //}

                    Parallel.ForEach(queries, queryName =>
                    {
                        InsertData(uri, queryName, tokenfinal);
                    });
                    SendEmail("Sucessful Sync with CMDB");
                    //Console.WriteLine("Time taken=" + sp.ElapsedMilliseconds.ToString());
                    //sp.Stop();
                    //sp.Reset();
                }
                
            }
            catch(Exception e)
            {
                SendEmail("Failure occured for CMDB Sync:" + e.Message.ToString());
            }
            
        }

        static async void SendEmail(string ExceptionMessage)
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

        public static List<string> GetColumns(string strConnection, string tableName)
        {
            SqlConnection conn = new SqlConnection(strConnection);
            List<string> columnList = new List<string>();
            DataTable dataTable = new DataTable();
            string cmdText = "Select top 1 * from Computer" ;
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

        private static DataTable SetColumnsOrder(DataTable table, List<string> columnNames)
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

        private static async void InsertData(string uri, string queryName, dynamic token)
        {
            dynamic data = "";
            DBHelper helper = new DBHelper();
            string json = "";
            
            while (!json.Contains("numberOfChunks"))
            {
                json = await helper.GetData(token, uri, queryName);
            }
            string connString = helper.GetConnectionString();
            data = JObject.Parse(json);

            for (int items = 1; items <= Convert.ToInt32(data.numberOfChunks); items++)
            {
                uri = ConfigurationManager.AppSettings.Get("Base_URI") + "/topology/result/" + data.queryResultId + "/" + items;
                json = "";
                DataSet ds = null;
                while (ds == null)
                {
                    json = await helper.GetData(token, uri, "");
                    ds = helper.GetProperties(json, "ucmdbId");
                }
                string test = string.Empty;
                if (!json.Contains("Computer") && !json.Contains("installed_software"))
                    test = "";
                //else
                //    continue;
                if (ds.Tables["Computer"] != null && ds.Tables["Computer"].Rows.Count > 0)
                {
                    List<string> columns = GetColumns(connString, "");
                    DataTable dataTable = new DataTable();
                    dataTable = SetColumnsOrder(ds.Tables["Computer"], columns);

                    helper.BulkInsert(connString, json, 0, "Computer", "ucmdbId", ds.Tables["Computer"]);
                }
                if (ds.Tables["InstalledSoftware"] != null && ds.Tables["InstalledSoftware"].Rows.Count > 0)
                    helper.BulkInsert(connString, json, 1, "InstalledSoftware", "ucmdbId", ds.Tables["InstalledSoftware"]);
                if (ds.Tables["Composition"] != null && ds.Tables["Composition"].Rows.Count > 0)
                    helper.BulkInsert(connString, json, 2, "Composition", "ucmdbId", ds.Tables["Composition"]);
                //helper.BulkInsert(connString, json, 2, "Mapping", "ucmdbId");//for mapping
            }
        }
    }
}

