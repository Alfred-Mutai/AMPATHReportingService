using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReportingService
{
    internal class GeneralSetup
    {
        public static string connectionString = ConfigurationManager.AppSettings["connectionString"].ToString();
        public static string Database = ConfigurationManager.AppSettings["Database"].ToString();
        public static string commandTimeout = ConfigurationManager.AppSettings["commandTimeout"].ToString();

        public class QueueModel
        {
            public int queueCount { get; set; }
        }
        public class paramTables
        {
            public string queueTable { get; set; }
            public string extractTable { get; set; }
            public string procedureName { get; set; }
        }
        public class paramQueries
        {
            public string truncateQueueQuery { get; set; }
            public string truncateExtractQuery { get; set; }
            public string buildQueueQuery { get; set; }
            public string buildQueryAddOn { get; set; }
        }
        public class Params
        {
            public string ConnectionsString { get; set; } = GeneralSetup.connectionString;
            public string Database { get; set; } = GeneralSetup.Database;
            public string queryType { get; set; } = "build";
            public int QuerySize { get; set; } = 100;
            public int commandTimeout { get; set; } = Int32.Parse(GeneralSetup.commandTimeout);
            public bool start { get; set; } = false;
            public bool hasLessParams { get; set; } = false;
            public MySqlParameter[] sqlParameters { get; set; }
            public bool complete { get; set; } = false;
            public bool truncateExtract { get; set; } = false;
            public paramTables tables { get; set; }
            public paramQueries queries { get; set; }
        }
        public enum DateRangeType
        {
            ReportDates,
            MonthReportDates,
            MetricsDates
        }
    }
}
