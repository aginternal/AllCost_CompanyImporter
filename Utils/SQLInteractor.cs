using DbImporterAllCost.Dto;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DbImporterAllCost.Utils
{
    public static class SQLInteractor
    {
        //gets all dbNames from and descriptions from the primavera db
        public static List<DbInfo> GetAllDbInfos(string connStr)
        {
            var query = $@"SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Codigo, IDNome from [PRIEMPRE].[dbo].[Empresas]')";
            var res = new List<DbInfo>();

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();
                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string empresa = reader.GetString(0);
                                string descricao = reader.GetString(1);

                                res.Add(new DbInfo(empresa, descricao));
                            }
                        }
                    }
                    connection.Close();
                }

                return res;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static void RestoreDataBase(string dbName, string connStr, string bakFileLocation, string dataFisicalPath, string logFisicalPath)
        {
			try
			{
                string dataFilePath = $@"{dataFisicalPath}{dbName}.mdf";
                string logFilePath = $@"{dataFisicalPath}{dbName}_Log.ldf";

                string restoreQuery = $@"
                    RESTORE DATABASE {dbName}
                    FROM DISK = '{bakFileLocation}'
                    WITH 
                        MOVE 'AllCost1' TO '{dataFilePath}',
                        MOVE 'AllCost1_log' TO '{logFilePath}',
                        REPLACE;
                ";

                using (var connection = new SqlConnection(connStr))
                {
                    connection.Open();
                    using (var command = new SqlCommand(restoreQuery, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    connection.Close();
                }
            }
			catch (Exception)
			{
				throw;
			}
        }

        public static void ImportAllData(string priName, string connStr, string ourName)
        {
            var priImporter = new PriImporter(priName, ourName);
            priImporter.ImportAllData(connStr);
        }
    }
}
