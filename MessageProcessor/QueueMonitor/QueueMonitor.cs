using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Databases;
using MessageProcessor;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace QueueMonitor
{
    public static class QueueMonitor
    {
        static string _mySqlconnectionString = Environment.GetEnvironmentVariable("MySqlConnectionString");
        static DatabaseMethods _databaseMethods = new DatabaseMethods(_mySqlconnectionString);
        [Function("DequeueingFunction")]
        public static void Run([QueueTrigger("jsons-queue", Connection = "")] string myQueueItem,
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
                        HandleSendingAndSavingOfEmail(emailAddressDbID,attributes);
                       
                    }
                }
            }  
        }
    
        private static Boolean  HandleSendingAndSavingOfEmail(int emailId, List<string> attributes)
        {
            Boolean returnResult = false;

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendFormat("Congratulation!{ 0}", Environment.NewLine);
            stringBuilder.Append("We have received following 10 unique attributes from you: ");
            stringBuilder.Append(string.Join(",", attributes));
            stringBuilder.Append(Environment.NewLine);
            stringBuilder.Append("Best regards, Millisecond");

            _databaseMethods.SaveEmailSentToDatabase(emailId, stringBuilder.ToString());

            returnResult = true;



            return returnResult;
        }
    }
}
