using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbImporterAllCost.Dto
{
    public class DbInfo
    {
        public string Empresa { get; set; }
        public string Descricao { get; set; }
        public string PriNome { get; set; }

        public DbInfo(string empresa, string descricao)
        {
            this.Empresa = empresa;
            this.Descricao = descricao;
            this.PriNome = $"PRI{empresa}";
        }
    }
}
