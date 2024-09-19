using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using StoreDataManager;
using QueryProcessor.Parser;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static OperationStatus Execute(string sentence)
        {
            /// The following is example code. Parser should be called
            /// on the sentence to understand and process what is requested
            

            if (sentence.StartsWith("CREATE DATABASE"))
            {
                string DataBaseName = sentence.Substring("CREATE DATABASE".Length).Trim();               
                return new CreateDataBase().Execute(DataBaseName);
            }
            if (sentence.StartsWith("SET DATABASE"))
            {
                string SetDataBaseName = sentence.Substring("SET DATABASE".Length).Trim();
                return new SetDataBase().Execute(SetDataBaseName);
                
            }
            if (sentence.StartsWith("CREATE TABLE"))
            {
                string TableInfo = sentence.Substring("CREATE TABLE".Length).Trim();
                string TableName = new ParserTable().GetTableName(TableInfo);
                string TableColumnsInfo = TableInfo.Substring(TableName.Length).Trim();
                List<Column> TableColumns = new ParserTable().GetColumns(TableColumnsInfo);

                return new CreateTable().Execute(TableName, TableColumns);  
                
            }   
            if (sentence.StartsWith("SELECT"))
            {
                return new Select().Execute();
            }
            
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }

       
    }
    
} 
