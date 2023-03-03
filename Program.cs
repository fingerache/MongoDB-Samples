using MongoDB.Bson;
using MongoDB.Driver;


namespace MDB_Changestream
{
    public static class MongoClientFactory
    { 
        private static MongoClient _client;
        private static Object lockobj;
        private static string connString = "<connection string>";
        public static MongoClient getMongoClient()
        {
            if (_client == null)
            {
                lockobj = new Object();
                lock (lockobj)
                {
                    if (_client == null)
                    {
                        _client = new MongoClient(connString);
                    }
                }
            }
            return _client;
        }
    }

    


    internal class Program
    {
        static PipelineDefinition<ChangeStreamDocument<BsonDocument>,
            ChangeStreamDocument<BsonDocument>> getInsertUpdateFilter()
        {
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
                    .Match(x => x.OperationType == ChangeStreamOperationType.Insert || 
                        x.OperationType == ChangeStreamOperationType.Update);

            return pipeline;
        }   

        static void Main(string[] args)
        {
            var dbclient = MongoClientFactory.getMongoClient();
            var database = dbclient.GetDatabase("<database name>");


            //using (var cursor = database.Watch())
                // OR use the following for more filtering
            using (var cursor = database.Watch(getInsertUpdateFilter()))
            {
                foreach(var change in cursor.ToEnumerable())
                {
                    Console.WriteLine(change.BackingDocument.ToString());
                }
            }

            Console.WriteLine("Exiting...");
            Console.ReadLine();
        }
    }
}