using System;

namespace MessageProcessor.Models
{
    public class ApiCallResult
    {
        public Boolean Success { get; set; } = false;
        public string Error_message { get; set; } = string.Empty;
      
    }
}
