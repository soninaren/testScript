using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.WebJobs.Host.Diagnostics;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TestScript
{
    public class DiagnosticEventTableStorageRepository
    {
        internal const string TableNamePrefix = "AzureFunctionsDiagnosticEvents";
        private readonly Timer _flushLogsTimer;

        private int _tableCreationRetries = 5;
        private ConcurrentDictionary<string, DiagnosticEvent> _events = new ConcurrentDictionary<string, DiagnosticEvent>();

        private CloudTableClient _tableClient;
        private CloudTable _diagnosticEventsTable;
        private readonly string _hostId;
        private object _syncLock = new object();
        private bool _disposed = false;
        private string _tableName;
        private readonly string _storageConnectionString;

        internal DiagnosticEventTableStorageRepository(string storage, string siteName, int logFlushInterval = 6000)
        {
            _storageConnectionString = storage ?? Environment.GetEnvironmentVariable("AzureWebjobsStorage");

            siteName = siteName ?? Environment.GetEnvironmentVariable("siteName");
            var hostId = siteName;
            if (hostId.Length > 32)
            {
                hostId = hostId[..32];
            }

            _hostId = hostId?.ToLowerInvariant().TrimEnd('-');
            _flushLogsTimer = new Timer(OnFlushLogs, null, logFlushInterval, logFlushInterval);
        }

        internal CloudTableClient TableClient
        {
            get
            {
                if (string.IsNullOrEmpty(_storageConnectionString))
                {
                    Console.WriteLine("Azure Storage connection string is empty or invalid. Unable to write diagnostic events.");
                }

                if (CloudStorageAccount.TryParse(_storageConnectionString, out CloudStorageAccount account))
                {
                    var tableClientConfig = new TableClientConfiguration();
                    _tableClient = new CloudTableClient(account.TableStorageUri, account.Credentials, tableClientConfig);
                }
                return _tableClient;
            }
        }

        internal ConcurrentDictionary<string, DiagnosticEvent> Events
        {
            get
            {
                return _events;
            }
        }

        internal CloudTable GetDiagnosticEventsTable(DateTime? now = null)
        {
            if (TableClient != null)
            {
                now = now ?? DateTime.UtcNow;
                string currentTableName = GetCurrentTableName(now.Value);

                // update the table reference when date rolls over to a new month
                if (_diagnosticEventsTable == null || currentTableName != _tableName)
                {
                    _tableName = currentTableName;
                    _diagnosticEventsTable = TableClient.GetTableReference(_tableName);
                }
            }

            return _diagnosticEventsTable;
        }

        private static string GetCurrentTableName(DateTime now)
        {
            return $"{TableNamePrefix}{now:yyyyMM}";
        }

        protected internal virtual async void OnFlushLogs(object state)
        {
            await FlushLogs();
        }

        internal virtual async Task FlushLogs(CloudTable table = null)
        {
            table = table ?? GetDiagnosticEventsTable();

            try
            {
                bool tableCreated = await TableStorageHelpers.CreateIfNotExistsAsync(table, _tableCreationRetries);
                if (tableCreated)
                {
                    TableStorageHelpers.QueueBackgroundTablePurge(table, TableClient, TableNamePrefix);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to create table '{table.Name}' after {_tableCreationRetries} retries. Aborting write operation {ex.Message}");

                // Clearing the memory cache to avoid memory build up.
                _events.Clear();
                return;
            }

            // Assigning a new empty directory to reset the event count in the new duration window.
            // All existing events are logged to other logging pipelines already.
            ConcurrentDictionary<string, DiagnosticEvent> tempDictionary = _events;
            _events = new ConcurrentDictionary<string, DiagnosticEvent>();
            if (tempDictionary.Count > 0)
            {
                await ExecuteBatchAsync(tempDictionary, table);
            }
        }

        internal async Task ExecuteBatchAsync(ConcurrentDictionary<string, DiagnosticEvent> events, CloudTable table)
        {
            try
            {
                var batch = new TableBatchOperation();
                foreach (string errorCode in events.Keys)
                {
                    TableOperation insertOperation = TableOperation.Insert(events[errorCode]);
                    await table.ExecuteAsync(insertOperation);
                    Console.WriteLine($"inserting {events[errorCode].LastTimeStamp}:{events[errorCode].ErrorCode}");
                }

                //foreach (var item in collection)
                //{

                //}
                //await table.ExecuteBatchAsync(batch);
                events.Clear();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Unable to write diagnostic events to table storage:{e.Message}");
            }
        }

        public void WriteDiagnosticEvent(DateTime timestamp, string errorCode, LogLevel level, string message, string helpLink, Exception exception)
        {
            if (string.IsNullOrEmpty(_hostId))
            {
                Console.WriteLine("Unable to write diagnostic events. Host id is set to null.");
                return;
            }

            var diagnosticEvent = new DiagnosticEvent(_hostId, timestamp)
            {
                ErrorCode = errorCode,
                HelpLink = helpLink,
                LastTimeStamp = timestamp,
                Timestamp = timestamp,
                Message = message,
                LogLevel = level,
                Details = exception?.ToFormattedString(),
                HitCount = 1
            };

            if (!_events.TryAdd(errorCode, diagnosticEvent))
            {
                lock (_syncLock)
                {
                    _events[errorCode].Timestamp = timestamp;
                    _events[errorCode].HitCount++;
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_flushLogsTimer != null)
                    {
                        _flushLogsTimer.Dispose();
                    }

                    FlushLogs().GetAwaiter().GetResult();
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
    }

    internal static class ExceptionExtensions
    {
        public static bool IsFatal(this Exception exception)
        {
            while (exception != null)
            {
                if ((exception is OutOfMemoryException && !(exception is InsufficientMemoryException)) ||
                    exception is ThreadAbortException ||
                    exception is AccessViolationException ||
                    exception is SEHException ||
                    exception is StackOverflowException)
                {
                    return true;
                }

                if (exception is TypeInitializationException &&
                    exception is TargetInvocationException)
                {
                    break;
                }

                exception = exception.InnerException;
            }

            return false;
        }

        public static string ToFormattedString(this Exception exception)
        {
            if (exception == null)
            {
                throw new ArgumentNullException(nameof(exception));
            }

            return ExceptionFormatter.GetFormattedException(exception);
        }
    }

    public class DiagnosticEvent : TableEntity
    {
        public DiagnosticEvent(string hostId, DateTime timestamp)
        {
            RowKey = TableStorageHelpers.GetRowKey(timestamp);
            PartitionKey = $"{hostId}-{timestamp:yyyyMMdd}";
        }

        public int HitCount { get; set; }

        public DateTime LastTimeStamp { get; set; }

        public string Message { get; set; }

        public string ErrorCode { get; set; }

        public string HelpLink { get; set; }

        public int Level { get; set; }

        [IgnoreProperty]
        public LogLevel LogLevel
        {
            get { return (LogLevel)Level; }
            set { Level = (int)value; }
        }

        public string Details { get; set; }
    }

    public class TableStorageHelpers
    {
        internal static string GetRowKey(DateTime now)
        {
            return string.Format("{0:D19}-{1}", DateTime.MaxValue.Ticks - now.Ticks, Guid.NewGuid());
        }

        internal static async Task<bool> CreateIfNotExistsAsync(CloudTable table, int tableCreationRetries, int retryDelayMS = 1000)
        {
            int attempt = 0;
            do
            {
                try
                {
                    return await table.CreateIfNotExistsAsync();
                }
                catch (StorageException e)
                {
                    // Can get conflicts with multiple instances attempting to create
                    // the same table.
                    // Also, if a table queued up for deletion, we can get a conflict on create,
                    // though these should only happen in tests not production, because we only ever
                    // delete OLD tables and we'll never be attempting to recreate a table we just
                    // deleted outside of tests.
                    if (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict &&
                        attempt < tableCreationRetries)
                    {
                        // wait a bit and try again
                        await Task.Delay(retryDelayMS);
                        continue;
                    }
                    throw;
                }
            }
            while (attempt++ < tableCreationRetries);

            return false;
        }

        internal static void QueueBackgroundTablePurge(CloudTable currentTable, CloudTableClient tableClient, string tableNamePrefix, int delaySeconds = 30)
        {
            var tIgnore = Task.Run(async () =>
            {
                try
                {
                    // the deletion is queued with a delay to allow for clock skew across
                    // instances, thus giving time for any in flight operations against the
                    // previous table to complete.
                    await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
                    await DeleteOldTablesAsync(currentTable, tableClient, tableNamePrefix);
                }
                catch (Exception e)
                {
                    // best effort - if purge fails we log and ignore
                    // we'll try again another time
                    Console.WriteLine("Error occurred when attempting to delete old diagnostic events tables." + e.Message);
                }
            });
        }

        internal static async Task DeleteOldTablesAsync(CloudTable currentTable, CloudTableClient tableClient, string tableNamePrefix)
        {
            var tablesToDelete = await ListOldTablesAsync(currentTable, tableClient, tableNamePrefix);
            Console.WriteLine($"Deleting {tablesToDelete.Count()} old tables.");
            foreach (var table in tablesToDelete)
            {
                Console.WriteLine($"Deleting table '{table.Name}'");
                await table.DeleteIfExistsAsync();
                Console.WriteLine($"{table.Name} deleted.");
            }
        }

        internal static async Task<IEnumerable<CloudTable>> ListOldTablesAsync(CloudTable currentTable, CloudTableClient tableClient, string tableNamePrefix)
        {
            var tables = await ListTablesAsync(tableClient, tableNamePrefix);
            return tables.Where(p => !string.Equals(currentTable.Name, p.Name, StringComparison.OrdinalIgnoreCase));
        }

        internal static async Task<IEnumerable<CloudTable>> ListTablesAsync(CloudTableClient tableClient, string tableNamePrefix)
        {
            List<CloudTable> tables = new List<CloudTable>();
            TableContinuationToken continuationToken = null;

            do
            {
                var results = await tableClient.ListTablesSegmentedAsync(tableNamePrefix, continuationToken);
                continuationToken = results.ContinuationToken;
                tables.AddRange(results.Results);
            }
            while (continuationToken != null);

            return tables;
        }
    }
}
