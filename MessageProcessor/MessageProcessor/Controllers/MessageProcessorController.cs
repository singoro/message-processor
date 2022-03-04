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
            ApiCallResult result = new ApiCallResult()
            {
                Success = false,
                Error_message = "",
                Return_code = 0
            };

              result = await StoreLogs(load.email);

            return result;

        }

        private async Task<ApiCallResult> StoreLogs(String email)
        {
            ApiCallResult result = new ApiCallResult()
            {
                Success = false,
                Error_message = "",
                Return_code = 0
            };

            try
            {
                        
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = blobClient.GetContainerReference("files");

                string fileName = email + "-" + DateTime.Now.ToShortDateString();
                var blob = container.GetAppendBlobReference(fileName);
                blob.Properties.ContentType = "text/csv";
                
                await blob.UploadTextAsync(email);

                result.Success = true;
                
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Error_message = ex.Message;
                _logger.LogError(ex, "Error occured while attempting to save to blob");
            }

            return result;
        }

    }
}
