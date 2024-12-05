using DbImporterAllCost.Utils;
using Microsoft.VisualBasic;

var connStr = "Server=85.10.135.117,14433;Database=BaseControle;User Id=sa;Password=@gile2024;TrustServerCertificate=True;";

//location of the .bak file we want to use to restore the new databases
var bakFile = @"C:\Agile\AllCost\AllCostDB.bak";

//this is the folder where the log files and the data files are stored
var defaultFisicalPath = @"C:\Program Files\Microsoft SQL Server\MSSQL16.ALLCOST\MSSQL\DATA\";

try
{
    // vamos dar loop por esta variavel e fzr as operações necessárias para cada obj na lista
    var dbInfos = SQLInteractor.GetAllDbInfos(connStr);

    SQLInteractor.RestoreDataBase("test", connStr, bakFile, defaultFisicalPath, defaultFisicalPath);
    SQLInteractor.ImportAllData("PRIALLCOST22", connStr, "test");

}
catch (Exception ex)
{
    string msg = ex.Message;
}