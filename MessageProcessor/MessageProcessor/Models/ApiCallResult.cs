using System;

namespace MessageProcessor.Models
{
    public class ApiCallResult
    {
        public int Return_code { get; set; }
        public string Error_message { get; set; }
        public Boolean Success { get; set; }
    }
}
