using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databases
{
    public class DatabaseMethods
    {
        private readonly string  _mySqöConnectionString;

        public DatabaseMethods(string dbConnectionString)
        {
            _mySqöConnectionString = dbConnectionString;
        }
        public Boolean IsAttributesEmailForGivenEmailOnGiveDateSent(int emailID, string date)
        {
            Boolean returnResult= false;

            MySqlConnection conn = new MySqlConnection(_mySqöConnectionString);
            conn.Open();
            string sqlStatement = string.Format("SELECT emailId FROM  messaging.sendemails WHERE emailId = {0} and date = '{1}'", emailID, date);
            using var cmd = new MySqlCommand(sqlStatement, conn);

            using MySqlDataReader rdr = cmd.ExecuteReader();

            while (rdr.Read())
            {
                if(rdr.GetInt32(0)>0)
                {
                    returnResult = true;
                }
            }

            return returnResult;

        }
        public List<string> AttributesForGivenEmailOnGivenDate(int emailID, string date)
        {
            List <string> attributes = new List <string> ();

            MySqlConnection conn = new MySqlConnection(_mySqöConnectionString);
            conn.Open();
            string sqlStatement = string.Format("SELECT attribute FROM  messaging.attributes WHERE attEmailId = {0} and date = '{1}'", emailID, date);
            using var cmd = new MySqlCommand(sqlStatement, conn);

            using MySqlDataReader rdr = cmd.ExecuteReader();

            while (rdr.Read())
            {
                attributes.Add(rdr.GetString(0));
            }

            return attributes;

        }
        public int SaveEmailAddressToDatabaseIfNotYetSaved(string emailAddress)
        {
            int returnResult = 0;

            try
            {
                MySqlConnection conn = new MySqlConnection(_mySqöConnectionString);
                conn.Open();
                MySqlCommand comm = conn.CreateCommand();
                comm.CommandText = "INSERT INTO messaging.emailaddresses (emailAddress) VALUES (@emailAddress)";
                comm.Parameters.AddWithValue("@emailAddress", emailAddress.Trim());
                comm.ExecuteNonQuery();
                conn.Close();
                returnResult = (int) comm.LastInsertedId;
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("emalAddresses_UNIQUE"))
                {
                    MySqlConnection conn = new MySqlConnection(_mySqöConnectionString);
                    conn.Open();
                    string sqlStatement = @"SELECT emailId FROM messaging.emailaddresses WHERE emailAddress =  '" + emailAddress +"'";
                    using var cmd = new MySqlCommand(sqlStatement, conn);

                    using MySqlDataReader rdr = cmd.ExecuteReader();

                    while (rdr.Read())
                    {
                        returnResult =rdr.GetInt32(0);
                    }
                }
                Console.WriteLine(ex.Message);
            }
            return returnResult;
        }
        public Boolean SaveAllEmailAttributesToDatabaseAtOnce(int attEmaId,  List<string> attributes)
        {
            Boolean returnResult = false;
            MySqlConnection mConnection = new MySqlConnection(_mySqöConnectionString);
            StringBuilder sCommand = new StringBuilder("INSERT INTO messaging.attributes (attEmailId, attribute,date) VALUES ");

            try
            {
                MySqlCommand comm = mConnection.CreateCommand();
                List<string> Rows = new List<string>();
                foreach(string attribute in attributes)
                {
                    Rows.Add(string.Format("('{0}','{1}','{2}')", MySqlHelper.EscapeString(attEmaId.ToString()), MySqlHelper.EscapeString(attribute.Trim()), MySqlHelper.EscapeString(DateTime.Now.ToString("yyyy.MM.dd"))));
                }
                sCommand.Append(string.Join(",", Rows));
                sCommand.Append(";");
                mConnection.Open();
                using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                {
                    myCmd.CommandType = CommandType.Text;
                    myCmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                if (attributes.Count >1 && ex.Message.Contains("Duplicate entry")) 
                {
                    foreach (string attribute in attributes)
                    {
                        SaveEmailOneAttributeToDatabases(attEmaId, attribute);
                    }
                }
                else
                {
                    Console.WriteLine(ex.Message);
                } 
            }
            return returnResult;
        }
        public Boolean SaveEmailSentToDatabase(int emailId, string emailBody)
        {
            Boolean returnResult = false;
            MySqlConnection mConnection = new MySqlConnection(_mySqöConnectionString);
            StringBuilder sCommand = new StringBuilder("INSERT INTO messaging.sendemails (emailId, emailtext,date) VALUES ");
            try
            {
                MySqlCommand comm = mConnection.CreateCommand();
                string sqlString = string.Format("('{0}','{1}','{2}');", MySqlHelper.EscapeString(emailId.ToString()), MySqlHelper.EscapeString(emailBody), MySqlHelper.EscapeString(DateTime.Now.ToString("yyyy.MM.dd")));
                sCommand.Append(sqlString);
                mConnection.Open();
                using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                {
                    myCmd.CommandType = CommandType.Text;
                    myCmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return returnResult;
        }
        private Boolean SaveEmailOneAttributeToDatabases(int attEmaId, string attribute)
        {
            Boolean returnResult = false;
            MySqlConnection mConnection = new MySqlConnection(_mySqöConnectionString);
            StringBuilder sCommand = new StringBuilder("INSERT INTO messaging.attributes (attEmailId, attribute,date) VALUES ");
            try
            {
                MySqlCommand comm = mConnection.CreateCommand();
                string sqlString = string.Format("('{0}','{1}','{2}');", MySqlHelper.EscapeString(attEmaId.ToString()), MySqlHelper.EscapeString(attribute.Trim()), MySqlHelper.EscapeString(DateTime.Now.ToString("yyyy.MM.dd")));
                sCommand.Append(sqlString);
                mConnection.Open();
                using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                {
                    myCmd.CommandType = CommandType.Text;
                    myCmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return returnResult;
        }

    }
}
