
using System.Collections.Generic;
using System.Text.Json;


namespace MessageProcessor
{
    public class DataLoad
    {
        public string Key { get; set; }
        public string Email { get; set; }
        public List<string> Attributes { get; set; }
    }
}
