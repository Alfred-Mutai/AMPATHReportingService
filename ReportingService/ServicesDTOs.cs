using MySql.Data.MySqlClient;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static ReportingService.GeneralSetup;

namespace ReportingService
{
    internal class ServicesDTOs
    {
        public static async Task<bool> BuildMetricsAsync()
        {
            bool reStatus = false;
            Params parameters = new Params();

            List<string> databases = new List<string>() { "ndwr_v1", "ndwr", "single_facility_ndwr" };
            foreach (var database in databases)
            {
                string truncateQueueQuery = $"TRUNCATE {database}.ndwr_flat_indicator_metrics";
                await ServicesDTOs.ExecuteNonQueryAsync(truncateQueueQuery, GeneralSetup.connectionString);

                var (status, message) = await ExecuteStoredProcedureWithRetriesAsync($"{database}.build_ndwr_flat_indicator_metrics", new MySqlParameter[] { new MySqlParameter("@param1", ServicesDTOs.GetDateRange(DateRangeType.MetricsDates).startDate), new MySqlParameter("@param2", ServicesDTOs.GetDateRange(DateRangeType.MetricsDates).endDate) }, parameters.ConnectionsString, parameters.commandTimeout);
                if (status)
                {
                    Console.WriteLine($"Metrics for {database} processed successfully.");
                }
                else
                {
                    Console.WriteLine($"Metrics: {message}");
                }
                reStatus = status;
            }

            return reStatus;
        }
        public static async Task<bool> BuildExtractAsync(Params parameters)
        {
            try
            {
                if (parameters.start)
                {
                    parameters.queries.truncateQueueQuery = $"TRUNCATE {parameters.Database}.{parameters.tables.queueTable}";
                    await ServicesDTOs.ExecuteNonQueryAsync(parameters.queries.truncateQueueQuery, parameters.ConnectionsString);
                    Console.WriteLine("Queue table truncated................");
                    if (parameters.truncateExtract)
                    {
                        parameters.queries.truncateExtractQuery = $"TRUNCATE {parameters.Database}.{parameters.tables.extractTable}";
                        await ServicesDTOs.ExecuteNonQueryAsync(parameters.queries.truncateExtractQuery, parameters.ConnectionsString);
                        Console.WriteLine("Extract table truncated................");
                    }
                    parameters.queries.buildQueueQuery = $@"REPLACE INTO {parameters.Database}.{parameters.tables.queueTable} {parameters.queries.buildQueryAddOn}";
                    await ServicesDTOs.ExecuteNonQueryAsync(parameters.queries.buildQueueQuery, parameters.ConnectionsString);
                    Console.WriteLine("Queue table build................");
                }

                // Get queue count
                int queueCount = await ServicesDTOs.GetQueryResultCountAsync(parameters.tables.queueTable, parameters, new MySqlParameter[] { new MySqlParameter("@querySize", parameters.QuerySize) });
                Console.WriteLine($"{queueCount} queues found................");

                // Process each queue batch
                for (int i = 1; i <= queueCount; i++)
                {
                    // Process queue for each batch
                    await ServicesDTOs.ProcessQueueAsync(
                        $"{parameters.Database}.{parameters.tables.procedureName}",
                        i,
                        parameters,
                        parameters.sqlParameters,
                        parameters.tables.queueTable
                    );
                }

                parameters.complete = true;
                Console.WriteLine($"Process completed successfully.");
            }
            catch (Exception ex)
            {
                logger.Error(ex, $"Error: {parameters.tables.procedureName}");
                Console.WriteLine($"Error: {ex.Message}");
            }

            return parameters.complete;
        }

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// Executes a non-query MySQL command (e.g., TRUNCATE, DELETE, REPLACE INTO).
        /// </summary>
        public static async Task ExecuteNonQueryAsync(string query, string connectionString)
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    using (var command = new MySqlCommand(query, connection))
                    {
                        command.CommandTimeout = Int32.Parse(GeneralSetup.commandTimeout);
                        await command.ExecuteNonQueryAsync();
                    }
                }
                catch (Exception ex)
                {
                    string mess = "Error executing non-query. Retrying.....";
                    Console.WriteLine($"{mess}: {ex.Message}");
                    logger.Error(ex, mess);
                    await ExecuteNonQueryAsync(query, connectionString);
                }
                return;
            }
        }

        /// <summary>
        /// Executes a stored procedure with retry logic, ensuring the same queue is retried on failure.
        /// </summary>
        public static async Task<(bool, string)> ExecuteStoredProcedureWithRetriesAsync(
            string storedProcedure,
            MySqlParameter[] parameters,
            string connectionString,
            int commandTimeout = 30)
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    using (var command = new MySqlCommand())
                    {
                        //command.CommandType = CommandType.StoredProcedure;
                        command.Connection = connection;
                        command.CommandTimeout = commandTimeout;

                        var parameterPlaceholders = new List<string>();
                        for (int i = 0; i < parameters.Count(); i++)
                        {
                            string paramName = $"@param{i + 1}";
                            parameterPlaceholders.Add(paramName);

                            // Add provided parameters or set missing ones to DBNull
                            var paramValue = i < parameters?.Length ? parameters[i].Value : DBNull.Value;
                            command.Parameters.AddWithValue(paramName, paramValue);
                        }

                        // Build the CALL statement with all placeholders
                        command.CommandText = $"CALL {storedProcedure}({string.Join(", ", parameterPlaceholders)})";

                        await command.ExecuteNonQueryAsync();
                    }

                    return (true, string.Empty);
                }
                catch (Exception ex)
                {
                    logger.Error(ex, "Error executing stored procedure");
                    return (false, ex.Message);
                }
            }
        }

        /// <summary>
        /// Processes a queue batch by batch, retrying failed batches until resolved.
        /// </summary>
        public static async Task ProcessQueueAsync(string storedProcedure, int batch, Params parameters, MySqlParameter[] queryParameters, string queueTable, int buildNo = 0)
        {
            Random random = new Random();
            if (buildNo == 0)
            {
                buildNo = random.Next(10000, 99999);
            }

            var (status, message) = await ExecuteStoredProcedureWithRetriesAsync(
                  storedProcedure,
                  queryParameters != null && queryParameters.Length > 0 ? queryParameters : ServicesDTOs.GetCommonMySqlParameters(parameters,buildNo),
                  parameters.ConnectionsString,
                  parameters.commandTimeout);

            if (status)
            {
                Console.WriteLine($"Batch {batch} executed on {DateTime.Now}.");
            }
            else
            {
                Console.WriteLine(message, $"Error processing batch {batch}. Retrying...");
                int personId = await LogFirstRowAsync($"SELECT person_id FROM {parameters.Database}.{queueTable}_{buildNo} ORDER BY person_id LIMIT 1", parameters.ConnectionsString);
                await ExecuteNonQueryAsync($"DELETE FROM {parameters.Database}.{queueTable}_{buildNo} WHERE person_id = '{personId}'", parameters.ConnectionsString);
                await ProcessQueueAsync(storedProcedure, batch, parameters, parameters.sqlParameters, queueTable, buildNo);
            }
        }

        /// <summary>
        /// Logs the first row of a specified table and returns the `person_id`.
        /// </summary>
        public static async Task<int> LogFirstRowAsync(string query, string connectionString)
        {
            int personId = 0;
            using (var connection = new MySqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    using (var command = new MySqlCommand(query, connection))
                    using (var reader = command.ExecuteReader())
                    {
                        if (await reader.ReadAsync())
                        {
                            personId = reader.GetInt32("person_id");
                            Console.WriteLine($"Logged person_id: {personId}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    string adonmess = "Error logging first row. Retrying.....";
                    Console.WriteLine($"{ex.Message}", adonmess);
                    logger.Error(ex, adonmess);

                    await LogFirstRowAsync(query, connectionString);
                }
                return personId;
            }
        }

        public static async Task<int> GetQueryResultCountAsync(string tableName, Params parameters, MySqlParameter[] queryParameters)
        {
            int count = 0;
            string query = $"SELECT CEILING(COUNT(*) / @querySize) FROM {parameters.Database}.{tableName}";
            using (var connection = new MySqlConnection(parameters.ConnectionsString))
            {
                try
                {
                    connection.Open();
                    using (var command = new MySqlCommand(query, connection))
                    {
                        command.CommandTimeout = Int32.Parse(GeneralSetup.commandTimeout);
                        // Add query parameters dynamically if any are provided
                        if (queryParameters != null && queryParameters.Length > 0)
                        {
                            command.Parameters.AddRange(queryParameters);
                        }

                        // Execute query and get result
                        object result = await command.ExecuteScalarAsync();

                        count = result != DBNull.Value ? Convert.ToInt32(result) : 0;
                    }
                }
                catch (Exception ex)
                {
                    string addonmess = "Error doing count. Retrying.....";
                    Console.WriteLine($"{ex.Message}", addonmess);

                    await GetQueryResultCountAsync(tableName, parameters, queryParameters);
                }
                return count;
            }
        }

        public static (string startDate, string endDate) GetDateRange(DateRangeType rangeType)
        {
            DateTime now = DateTime.Now;

            switch (rangeType)
            {
                case DateRangeType.ReportDates:
                    DateTime firstDayOfLastMonth = new DateTime(now.Year, now.Month, 1).AddMonths(-1);
                    DateTime firstDayOfCurrentMonth = new DateTime(now.Year, now.Month, 1);
                    return (firstDayOfLastMonth.ToString("yyyy-MM-dd HH:mm:ss"), firstDayOfCurrentMonth.ToString("yyyy-MM-dd HH:mm:ss"));

                case DateRangeType.MonthReportDates:
                    DateTime firstDayOfPrevMonth = new DateTime(now.Year, now.Month, 1).AddMonths(-1);
                    DateTime lastDayOfPrevMonth = new DateTime(now.Year, now.Month, 1).AddDays(-1);
                    return (firstDayOfPrevMonth.ToString("yyyy-MM-dd HH:mm:ss"), lastDayOfPrevMonth.ToString("yyyy-MM-dd HH:mm:ss"));

                case DateRangeType.MetricsDates:
                    DateTime lastMonthBaseDate = now.AddMonths(-1);
                    DateTime lastMonthEndDate = new DateTime(lastMonthBaseDate.Year, lastMonthBaseDate.Month, DateTime.DaysInMonth(lastMonthBaseDate.Year, lastMonthBaseDate.Month));
                    DateTime twoMonthsAgoEndDate = lastMonthEndDate.AddMonths(-1);
                    return (twoMonthsAgoEndDate.ToString("yyyy-MM-dd HH:mm:ss"), lastMonthEndDate.ToString("yyyy-MM-dd HH:mm:ss"));

                default:
                    throw new ArgumentException("Invalid DateRangeType specified.");
            }
        }

        public static string GetMLWeek()
        {
            DateTime now = DateTime.Now;

            // Calculate the date for the previous week
            DateTime previousWeekDate = now.AddDays(-7);

            // Get the ISO 8601 week number and year
            Calendar calendar = CultureInfo.InvariantCulture.Calendar;

            // Get the week number
            int weekNumber = calendar.GetWeekOfYear(
                previousWeekDate,
                CalendarWeekRule.FirstFourDayWeek,
                DayOfWeek.Monday
            );

            // Get the year part of the ISO week date
            int year = calendar.GetYear(previousWeekDate);

            // Return the formatted string
            return $"{year}-W{weekNumber:D2}";
        }

        public static MySqlParameter[] GetCommonMySqlParameters(Params parameters, int buildNo)
        {
            var procedureParameters = new List<MySqlParameter>
            {
                new MySqlParameter("@param1", parameters.queryType),
                new MySqlParameter("@param2", buildNo),
                new MySqlParameter("@param3", parameters.QuerySize)
            };
            if (!parameters.hasLessParams)
            {
                procedureParameters.Add(new MySqlParameter("@param4", 1));
            }
            procedureParameters.Add(new MySqlParameter($"@param{(parameters.hasLessParams ? 4 : 5)}", "false"));

            return procedureParameters.ToArray();
        }
    }
}
