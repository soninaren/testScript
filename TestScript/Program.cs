using Microsoft.Extensions.Logging;
using System;
using System.Threading;

namespace TestScript
{
    internal class Program
    {
        static void Main(string[] args)
        {

            var storage = Environment.GetEnvironmentVariable("AzureWebjobsStorage");
            var siteName = Environment.GetEnvironmentVariable("siteName");

            if (string.IsNullOrEmpty(storage) || string.IsNullOrEmpty(siteName))
            {
                Console.WriteLine("Please make sure AzureWebjobsStorage and siteName environment variable are set");
            }

            var repo = new DiagnosticEventTableStorageRepository(storage, siteName);

            Random r = new Random();
            for (int i = 0; i < 100; i++)
            {
                int code = r.Next(1, 10);
                string errorCode = "AZF" + code;
                string message = $"Message for {errorCode}";
                var timestamp = DateTime.Now.AddHours(0 - (r.NextDouble() * 24));
                string helpLink = $"https:\\de\\{errorCode}";
                Exception e = new Exception($"Exception for {errorCode}");
                LogLevel level = (LogLevel)(code % 4);

                double hoursAdded = 0 - (r.NextDouble() * 24);
                Console.WriteLine(timestamp);
                repo.WriteDiagnosticEvent(timestamp, errorCode, level, message, helpLink, e);

                Thread.Sleep(code * 120);
            }
        }
    }
}
