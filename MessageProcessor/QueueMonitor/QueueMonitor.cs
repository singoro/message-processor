using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Databases;
using MessageProcessor;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace QueueMonitor
{
    public static class QueueMonitor
    {
        static string _mySqlconnectionString = Environment.GetEnvironmentVariable("MySqlConnectionString");
        static DatabaseMethods _databaseMethods = new DatabaseMethods(_mySqlconnectionString);
        [Function("DequeueingFunction")]
        public static async void Run([QueueTrigger("jsons-queue", Connection = "")] string myQueueItem,
            FunctionContext context)
        {
            var logger = context.GetLogger("DequeueingFunction");
           
            logger.LogInformation($"C# Queue trigger function processed: {myQueueItem}");
            DataLoad dataLoad = JsonSerializer.Deserialize<DataLoad>(myQueueItem);
            int emailAddressDbID = 0;
            emailAddressDbID = _databaseMethods.SaveEmailAddressToDatabaseIfNotYetSaved(dataLoad.Email);
            if(emailAddressDbID>0)
            {
                _databaseMethods.SaveAllEmailAttributesToDatabaseAtOnce(emailAddressDbID, dataLoad.Attributes);

                if (!_databaseMethods.IsAttributesEmailForGivenEmailOnGiveDateSent(emailAddressDbID, DateTime.Now.ToString("yyyy.MM.dd")))
                {
                    List <string> attributes = new List <string>();
                    attributes = _databaseMethods.AttributesForGivenEmailOnGivenDate(emailAddressDbID, DateTime.Now.ToString("yyyy.MM.dd"));

                    if(attributes.Count>9)
                    {
                        await HandleSendingAndSavingOfEmail(emailAddressDbID,attributes);
                       
                    }
                }
            }  
        }
    
        private static async Task<Boolean> HandleSendingAndSavingOfEmail(int emailId, List<string> attributes)
        {
            Boolean returnResult = false;

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendFormat("Congratulation!{ 0}", Environment.NewLine);
            stringBuilder.Append("We have received following 10 unique attributes from you: ");
            stringBuilder.Append(string.Join(",", attributes));
            stringBuilder.Append(Environment.NewLine);
            stringBuilder.Append("Best regards, Millisecond");

            _databaseMethods.SaveEmailSentToDatabase(emailId, stringBuilder.ToString());
            await WriteLogToAzureContainer( stringBuilder.ToString() + "[" + emailId + "]");
            returnResult = true;



            return returnResult;
        }

        private static async Task<Boolean> WriteLogToAzureContainer(  string newDataValue)
        {
            Boolean returnResult = false;
            string containerName = Environment.GetEnvironmentVariable("AzureSettings:ContainerName".ToString());
            string logFileName = "sentemails.log"; 
            var storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("ContainerName"));
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
