using Newtonsoft.Json.Linq;
using System;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;

namespace CMDB_SMAX_Integration
{
    class Program
    {
        static async Task Main(string[] args)
        {
            DBHelper helper = new DBHelper();
            try
            {
                //Truncate all data
                string query = "dbo.truncate_ComputerSoftware";
                DataTable dtRun = new DataTable();
                dtRun = helper.ExecuteSelectQuery(query, null);

                int chunk_size = Convert.ToInt32(ConfigurationManager.AppSettings.Get("CHUNK_SIZE"));
                string baseURI = ConfigurationManager.AppSettings.Get("Base_URI");
                string queryNames = ConfigurationManager.AppSettings.Get("QueryName");
                string[] queries = queryNames.Split(',');
                string tableName = ConfigurationManager.AppSettings.Get("TableName");

                //get authentication token
                //check for error like SMAX
                var token = "";
                while (!token.Contains("token"))
                {
                    token = await helper.GetToken();
                }
                dynamic data = "";
                data = JObject.Parse(token);
                dynamic tokenfinal = data.token;
                
                if (queries.Length > 0)
                {
                    Stopwatch sp = new Stopwatch();
                    sp.Start();
                    //Uncomment for loop below if needs to debug and comment Parallel ForEaach
                    //foreach (string queryName in queries)
                    //{
                    //    helper.InsertData(baseURI, chunk_size, queryName, tableName, tokenfinal);
                    //}
                    Parallel.ForEach(queries, queryName =>
                    {
                        helper.InsertData(baseURI, chunk_size, queryName, tableName, tokenfinal);
                    });
                    helper.SendEmail("Successful Sync with CMDB!");
                    Console.WriteLine("Time taken=" + sp.ElapsedMilliseconds.ToString());
                    sp.Stop();
                    sp.Reset();
                }
            }
            catch(Exception e)
            {
                helper.SendEmail("Failure occured for CMDB Sync:" + e.Message.ToString());
            }
        }
    }
}
