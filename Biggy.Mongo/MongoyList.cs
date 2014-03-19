using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace Biggy.Mongo
{
    public class MongoyList<T> : InMemoryList<T> where T: new()
    {
        private const string MongoIdElementName = "_id";
        public MongoyList(string host,                           
                          string database = "biggy", 
                          string collection = "list",
                          int port = 27017,
                          string username=null, 
                          string password=null)
        {
            Initialize(host, port, database, collection, username, password);
            Reload();
            FireLoadedEvents();
        }

        public override void Add(T thing)
        {
            _collection.Insert(thing);
            base.Add(thing);
        }

        public void Add(ICollection<T> things)
        {
            _collection.InsertBatch(things);
            foreach (var thing in things)
            {
                base.Add(thing);
            }
        }

        /// <summary>
        /// Drops all data from the MongoDB Collection - BEWARE
        /// </summary>
        public override void Clear()
        {
            _collection.RemoveAll();
            base.Clear();
        }

        public void Flush()
        {
            foreach (var item in _items)
            {
                _collection.Save(item);
            }
        }

        public void Reload()
        {
            _items = _collection.FindAll().ToList();
        }

        public override bool Remove(T thing)
        {
            var query = GetIDQuery(thing);
            _collection.Remove(query);
            return _items.Remove(thing);
        }

        private static IMongoQuery GetIDQuery(T thing)
        {
            if (BsonClassMap.IsClassMapRegistered(typeof(T)))
            {
                var classMap = BsonClassMap.LookupClassMap(typeof(T));
                if (classMap.IdMemberMap != null)
                {
                    var memberMap = classMap.GetMemberMapForElement(MongoIdElementName);
                    var idValue = (BsonValue)memberMap.Getter(thing);
                    return Query.EQ(MongoIdElementName, idValue);
                }
            }

            // Default - use Id property
            // This will throw an exception if your object does not have an Id property
            return Query.EQ(MongoIdElementName, ((dynamic)thing).Id);
        }

        public override int Update(T thing)
        {
            var query = GetIDQuery(thing);
            _collection.Update(query, MongoDB.Driver.Builders.Update.Replace(thing), UpdateFlags.Upsert);
            return base.Update(thing);
        }

        private void Initialize(string host, int port, string database, string collection, string username, string password)
        {
            var clientSettings = CreateClientSettings(host, port, database, username, password);
            _client = new MongoClient(clientSettings);
            _server = _client.GetServer();
            _database = _server.GetDatabase(database);
            _collection = _database.GetCollection<T>(collection);
        }

        private static MongoClientSettings CreateClientSettings(string host, int port, string database, 
                                                                string username, string password)
        {
            var clientSettings = new MongoClientSettings();
            clientSettings.Server = new MongoServerAddress(host, port);
            if (!String.IsNullOrEmpty(username) && !String.IsNullOrEmpty(password))
            {
                var credential = MongoCredential.CreateMongoCRCredential(database, username, password);
                clientSettings.Credentials = new[] {credential};
            }
            return clientSettings;
        }


        private MongoClient _client;
        private MongoServer _server;
        private MongoDatabase _database;
        private MongoCollection<T> _collection;
    }
}