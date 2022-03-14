using System;
using System.Text.Json;
using Databases;
using MessageProcessor;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace QueueMonitor
{
    public static class QueueMonitor
    {
        [Function("DequeueingFunction")]
        public static void Run([QueueTrigger("jsons-queue", Connection = "")] string myQueueItem,
            FunctionContext context)
        {
            var logger = context.GetLogger("DequeueingFunction");
            logger.LogInformation($"C# Queue trigger function processed: {myQueueItem}");

            DataLoad dataLoad = JsonSerializer.Deserialize<DataLoad>(myQueueItem);

            DatabaseMethods databaseMethods = new DatabaseMethods();
            int emailAddressDbID = 0;
            emailAddressDbID=databaseMethods.SaveEmailAddressToDatabaseIfNotYetSaved(dataLoad.Email);
            if(emailAddressDbID>0)
            {
                databaseMethods.SaveAllEmailAttributesToDatabaseAtOnce(emailAddressDbID, dataLoad.Attributes);
            }
           
        }
    }
}
