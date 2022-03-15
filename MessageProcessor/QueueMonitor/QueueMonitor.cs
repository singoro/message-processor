using Databases;
using MessageProcessor;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace QueueMonitor
{
    public static class QueueMonitor
    {
        static readonly string  _mySqlconnectionString = Environment.GetEnvironmentVariable("MySqlConnectionString");
        private static string _connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
        static readonly DatabaseMethods _databaseMethods = new(_mySqlconnectionString);
        [Function("DequeueingFunction")]
        public static async void Run([QueueTrigger("jsons-queue", Connection = "")] string myQueueItem,
            FunctionContext context)
        {
            var logger = context.GetLogger("DequeueingFunction");
            logger.LogInformation($"C# Queue trigger function processed: {myQueueItem}");
            DataLoad dataLoad = JsonSerializer.Deserialize<DataLoad>(myQueueItem);
            int emailAddressDbID;
            emailAddressDbID = _databaseMethods.SaveEmailAddressToDatabaseIfNotYetSaved(dataLoad.Email);
            if (emailAddressDbID > 0)
            {
                _databaseMethods.SaveAllEmailAttributesToDatabaseAtOnce(emailAddressDbID, dataLoad.Attributes);

                if (!_databaseMethods.IsAttributesEmailForGivenEmailOnGiveDateSent(emailAddressDbID, DateTime.Now.ToString("yyyy.MM.dd")))
                {
                    List<string> attributes = _databaseMethods.AttributesForGivenEmailOnGivenDate(emailAddressDbID, DateTime.Now.ToString("yyyy.MM.dd"));
                    if (attributes.Count > 9)
                    {
                        await HandleSendingAndSavingOfEmail(emailAddressDbID, attributes);

                    }
                }
            }
        }

        private static async Task<Boolean> HandleSendingAndSavingOfEmail(int emailId, List<string> attributes)
        {
            Boolean returnResult;
            try
            {
                StringBuilder stringBuilder = new();
                stringBuilder.AppendFormat("Congratulation!{0}", Environment.NewLine);
                stringBuilder.Append("We have received following 10 unique attributes from you: ");
                stringBuilder.Append(string.Join(",", attributes));
                stringBuilder.Append(Environment.NewLine);
                stringBuilder.Append("Best regards, Millisecond");
                _databaseMethods.SaveEmailSentToDatabase(emailId, stringBuilder.ToString());
                await WriteLogToAzureContainer(stringBuilder.ToString() + "[" + emailId + "]");
                returnResult = true;
            }
            catch (Exception ex)
            {
                returnResult = false;
                Console.WriteLine(ex.Message);
            }
            return returnResult;
        }

        private static async Task<Boolean> WriteLogToAzureContainer(string newDataValue)
        {
            Boolean returnResult;
            string containerName = Environment.GetEnvironmentVariable("ContainerName".ToString());
            string logFileName = "sentemails.log";
            var storageAccount = CloudStorageAccount.Parse(_connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            try
            {
                var container = blobClient.GetContainerReference(containerName.ToString());
                var blob = container.GetAppendBlobReference(logFileName);
                bool isPresent = await blob.ExistsAsync();
                if (!isPresent)
                {
                    await blob.CreateOrReplaceAsync();
                }
                await blob.AppendTextAsync($"{newDataValue} \n");
                returnResult = true;
            }
            catch (Exception ex)
            {
                returnResult = false;
                Console.WriteLine(ex.Message);
            }
            return returnResult;
        }

    }
}
