using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbImporterAllCost.Dto
{
    public class DbImportationLog
    {
        public string TableName { get; set; }
        public string Status { get; set; }
        public string Message { get; set; }
        public DateTime Date { get; set; }

        public DbImportationLog(string tblName, string status, string msg)
        {
            this.TableName = tblName;
            this.Status = status;
            this.Message = msg;
            this.Date = DateTime.Now;
        }
    }
}
