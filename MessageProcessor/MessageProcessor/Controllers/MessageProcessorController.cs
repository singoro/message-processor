using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using MessageProcessor.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Threading.Tasks;

namespace MessageProcessor.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MessageProcessorController : ControllerBase
    {
        private readonly ILogger<MessageProcessorController> _logger;
        private string connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
        public MessageProcessorController(ILogger<MessageProcessorController> logger)
        {
            _logger = logger;
        }
        [HttpPost]
        public async Task<ApiCallResult> ReceiveMessageAsync([FromBody] DataLoad load)
        {
            ApiCallResult result = new ApiCallResult();
          
            result = await SendToLogs(load);

            return result;
        }

        private async Task<ApiCallResult> SendToLogs(DataLoad load)
        {
            ApiCallResult returnResult = new ApiCallResult();
           
            try
            {

                string containerName = "files";
                string logFileName = DateTime.Now.ToString("yyyyMMdd") + " - " + load.email + ".log"; //added date string to ensure that everyday a new file is created for every email that comes in
                var storageAccount = CloudStorageAccount.Parse(connectionString);
                var blobClient = storageAccount.CreateCloudBlobClient();
                await WriteLogToAzureContainer(containerName, logFileName, blobClient, load.Key);

                returnResult.Success = true;
                
            }
            catch (Exception ex)
            {
                returnResult.Error_message = ex.Message;
                _logger.LogError(ex, ":Error occured while attempting to send to logs");
            }

            return returnResult;
        }


        private async Task<ApiCallResult> WriteLogToAzureContainer(string containerName, string logFileName, CloudBlobClient objBlobClient, string newDataValue)
        {
            ApiCallResult returnResult = new ApiCallResult();
            try
            {
                var container = objBlobClient.GetContainerReference(containerName.ToString());
                var blob = container.GetAppendBlobReference(logFileName);
                bool isPresent = await blob.ExistsAsync();

                if (!isPresent)
                {
                    await blob.CreateOrReplaceAsync();
                }

                await blob.AppendTextAsync($"{newDataValue} \n");
            }
            catch (Exception ex)
            {
                returnResult.Error_message = ex.Message;
                _logger.LogError(ex, " An Error occured while attempting to save to blob");
            }

            return returnResult;
        }

    }
}
