using DbImporterAllCost.Dto;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace DbImporterAllCost.Utils
{
    public static class Logger
    {
        public static void Commit(this DbImportationLog log, string connStr)
        {
            var query = $@"
                        INSERT INTO CRM_DB_IMPORT_LOGS(Message, Date, TableName, Status)
                        VALUES (@msg, @date, @tblName, @status)
                        ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@msg", log.Message);
                        command.Parameters.AddWithValue("@date", log.Date);
                        command.Parameters.AddWithValue("@tblName", log.TableName);
                        command.Parameters.AddWithValue("@status", log.Status);

                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                string msg = ex.Message;
            }
        }
    }
}
