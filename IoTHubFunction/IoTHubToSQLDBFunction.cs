using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Net.Http;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;
using System;
using System.Data.SqlClient;
using System.Data;

/// <summary>
/// IntelliSense AzureFunction Job for Xserver.IoT
/// </summary>

namespace AzureFunctionApp
{
    public static class IoTHubFunction
    {
        private static HttpClient client = new HttpClient();

        [FunctionName("IoTHubToSQLDB")]
        public static void Run([IoTHubTrigger("messages/events", Connection = "IoTHub")] EventData message, ILogger log)
        {
#if (DEBUG)
            log.LogInformation($"IoT Hub trigger function processed a message: {Encoding.UTF8.GetString(message.Body.Array)}");  //Debug log
#endif

            try
            {
                string MessageType = null;
                string Namespace = null;

                foreach (var pkey in message.Properties.Keys)
                {
                    if (pkey == "T")
                    {
                        MessageType = message.Properties[pkey].ToString();
                    }
                    else if (pkey == "N")
                    {
                        Namespace = message.Properties[pkey].ToString();
                    }
                }

                if (MessageType != null && Namespace != null)
                {
                    var Message = Encoding.UTF8.GetString(message.Body.Array);
                    string error = null;
                    if (Message != null)
                    {
                        if (MessageType == "Per")
                        {
                            error = RunSql("TransferDataFromJsonPeriodLogs", Message, Namespace);
                        }
                        else if (MessageType == "Dif")
                        {
                            error = RunSql("TransferDataFromJsonDiffLogs", Message, Namespace);
                        }
                        else if (MessageType == "Ev")
                        {
                            error = RunSql("TransferDataFromJsonEvents", Message, Namespace);
                        }
                        else if (MessageType == "Al")
                        {
                            error = RunSql("TransferDataFromJsonAlarmLogs", Message, Namespace);
                        }
                        else if (MessageType == "Sn")
                        {
                            error = RunSql("TransferDataFromJsonSnapshots", Message, Namespace);
                        }

                        if (error != null)
                        {
#if (DEBUG)
                            log.LogInformation($"IoT Hub trigger function processed a message - RunSql: {error}");
#endif
                        }
                    }
                }
            }
            catch (Exception ex)
            {
#if (DEBUG)
                log.LogInformation($"IoT Hub trigger function processed a message: {ex.Message}");
#endif
            }
        }

        [SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Method already uses a Stored Procedure")]
        private static string RunSql(string spName, string json, string nsName = "")
        {
            try
            {
                var connstr = System.Environment.GetEnvironmentVariable("sqldb_connection");

                using (SqlConnection conn = new SqlConnection(connstr))
                {
                    using (SqlCommand cmd = new SqlCommand(spName, conn) { CommandType = CommandType.StoredProcedure })
                    {
                        cmd.Parameters.Add("@json", SqlDbType.NVarChar, -1).Value = json;
                        if (!String.IsNullOrWhiteSpace(nsName))
                        {
                            cmd.Parameters.Add("@par_NS", SqlDbType.NVarChar, 200).Value = nsName;
                        }

                        conn.Open();
                        cmd.ExecuteNonQuery();
                    }
                }
                return null;
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }

    }
}