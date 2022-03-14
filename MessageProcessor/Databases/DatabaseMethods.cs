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
        private static MySqlConnection _mySqlAsyncConn;
        internal static MySqlConnection MySqlAsyncConn
        {
            get { return _mySqlAsyncConn; }
            set { _mySqlAsyncConn = value; }
        }

        private string  _ConnectionString = @"server=walu-test.mysql.database.azure.com;user=walu;database=messaging; port=3306;password=eseOyuBaya09";
        public int SaveEmailAddressToDatabaseIfNotYetSaved(string emailAddress)
        {
            int returnResult = 0;

            try
            {
                MySqlConnection conn = new MySqlConnection(_ConnectionString);
                conn.Open();
                MySqlCommand comm = conn.CreateCommand();
                comm.CommandText = "INSERT INTO messaging.email_addresses (emailAddress) VALUES (@emailAddress)";
                comm.Parameters.AddWithValue("@emailAddress", emailAddress.Trim());
                comm.ExecuteNonQuery();
                conn.Close();
                returnResult = (int) comm.LastInsertedId;
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("emalAddresses_UNIQUE"))
                {
                    MySqlConnection conn = new MySqlConnection(_ConnectionString);
                    conn.Open();
                    string sqlStatement = @"SELECT emailId FROM messaging.email_addresses WHERE emailAddress =  '" + emailAddress +"'";
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
            MySqlConnection mConnection = new MySqlConnection(_ConnectionString);
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

        private Boolean SaveEmailOneAttributeToDatabases(int attEmaId, string attribute)
        {
            Boolean returnResult = false;
            MySqlConnection mConnection = new MySqlConnection(_ConnectionString);
            StringBuilder sCommand = new StringBuilder("INSERT INTO messaging.attributes (attEmaId, attribute,date) VALUES ");
            try
            {
                MySqlCommand comm = mConnection.CreateCommand();
                string sqlString=string.Format("('{0}','{1}','{2}');", MySqlHelper.EscapeString(attEmaId.ToString()), MySqlHelper.EscapeString(attribute.Trim()), MySqlHelper.EscapeString(DateTime.Now.ToString("yyyy.MM.dd")));
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
