using Azure.Storage.Queues;
using MessageProcessor.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Nancy.Json;
using System;
using System.Threading.Tasks;

namespace MessageProcessor.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MessageProcessorController : ControllerBase
    {
        private readonly ILogger<MessageProcessorController> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");

        public MessageProcessorController(ILogger<MessageProcessorController> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }
        [HttpPost]
        public async Task<ApiCallResult> ReceiveMessageAsync([FromBody] DataLoad dataLoad)
        {
            ApiCallResult apiResultFromSendingLogs;
            ApiCallResult apiResultFromSendingToQueue;

            var loadJson = new JavaScriptSerializer().Serialize(dataLoad);

            apiResultFromSendingLogs = await SendToLogs(dataLoad);
            apiResultFromSendingToQueue = await SendMessageToQueue(loadJson);

            if (apiResultFromSendingLogs.Success && apiResultFromSendingToQueue.Success)
            {
                return apiResultFromSendingLogs;
            }
            else if (!apiResultFromSendingLogs.Success)
            {
                return apiResultFromSendingLogs;
            }
            else
            {
                return apiResultFromSendingToQueue;
            }
        }

        private async Task<ApiCallResult> SendToLogs(DataLoad dataLoad)
        {
            ApiCallResult returnResult = new();

            try
            {
                string containerName = _configuration["AzureSettings:ContainerName"].ToString();
                string logFileName = DateTime.Now.ToString("yyyyMMdd") + " - " + dataLoad.Email + ".log"; // today's date string precedes the file and this ensures that everyday a new file is created for every email that comes in
                var storageAccount = CloudStorageAccount.Parse(_connectionString);
                var blobClient = storageAccount.CreateCloudBlobClient();
                var loadJson = new JavaScriptSerializer().Serialize(dataLoad);
                returnResult = await WriteLogToAzureContainer(containerName, logFileName, blobClient, loadJson);
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
            ApiCallResult returnResult = new();
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
                returnResult.Success = true;
            }
            catch (Exception ex)
            {
                returnResult.Error_message = ex.Message;
                _logger.LogError(ex, " An Error occured while attempting to save to blob");
            }

            return returnResult;
        }

        private async Task<ApiCallResult> SendMessageToQueue(string message)
        {
            string queueName = _configuration["AzureSettings:QueueName"].ToString();

            ApiCallResult returnResult = new();
            try
            {
                QueueClient queueClient = new(_connectionString, queueName);
                queueClient.CreateIfNotExists();

                if (queueClient.Exists())
                {
                    await queueClient.SendMessageAsync(message);
                    returnResult.Success = true;
                }
            }
            catch (Exception ex)
            {
                returnResult.Error_message = ex.Message;
                _logger.LogError(ex, ":Error occured while attempting to send to data to the queue"); throw;
            }

            return returnResult;
        }

    }
}
