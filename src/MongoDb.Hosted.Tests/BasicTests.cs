using MongoDB.Driver;
using MongoDB.Hosted;
using NUnit.Framework;
using System.Threading.Tasks;

namespace MongoDB.Hosted.Tests
{
    [TestFixture]
    public class BasicTests
    {
        [Test]
        public void BasicStartupTests()
        {
            using (var embedded = new HostedMongoDbServer())
            {
                var client = embedded.Client;
            }
        }

        private class TestClass
        {
            public int Id { get; set; }
            public string TestValue { get; set; }
        }

        [Test]
        public async Task ReadWriteTest()
        {
            using (var embedded = new HostedMongoDbServer())
            {
                var client = embedded.Client;
                var db = client.GetDatabase("test");
                var collection = db.GetCollection<TestClass>("col");
                await collection.InsertOneAsync(new TestClass() { Id = 12345, TestValue = "Hello world." });
                var retrieved = await collection.Find(x => x.Id == 12345).SingleOrDefaultAsync();
                Assert.NotNull(retrieved, "No object came back from the database.");
                Assert.AreEqual("Hello world.", retrieved.TestValue, "Unexpected test value came back.");
            }
        }

        [Test]
        public async Task DualServerReadWriteTest()
        {
            using (var embedded1 = new HostedMongoDbServer())
            using (var embedded2 = new HostedMongoDbServer())
            {
                var client1 = embedded1.Client;
                var db1 = client1.GetDatabase("test");
                var collection1 = db1.GetCollection<TestClass>("col");
                await collection1.InsertOneAsync(new TestClass() { Id = 12345, TestValue = "Hello world." });
                var retrieved1 = await collection1.Find(x => x.Id == 12345).SingleOrDefaultAsync();
                Assert.NotNull(retrieved1, "No object came back from the database.");
                Assert.AreEqual("Hello world.", retrieved1.TestValue, "Unexpected test value came back.");

                var client2 = embedded2.Client;
                var db2 = client2.GetDatabase("test");
                var collection2 = db2.GetCollection<TestClass>("col");
                await collection2.InsertOneAsync(new TestClass() { Id = 12345, TestValue = "Hello world." });
                var retrieved2 = await collection2.Find(x => x.Id == 12345).SingleOrDefaultAsync();
                Assert.NotNull(retrieved2, "No object came back from the database.");
                Assert.AreEqual("Hello world.", retrieved2.TestValue, "Unexpected test value came back.");
            }
        }

        [Test]
        public async Task PersistedServedKeepsData()
        {
            try
            {


                using (var embedded = new HostedMongoDbServer(dbPath: "./db", persistent: true))
                {
                    var client1 = embedded.Client;
                    var db1 = client1.GetDatabase("test");
                    var collection1 = db1.GetCollection<TestClass>("col");
                    await collection1.InsertOneAsync(new TestClass() { Id = 12345, TestValue = "Hello world." });
                    System.Threading.Thread.Sleep(1000);// time for the journal to be stored
                }

                using (var embedded = new HostedMongoDbServer(dbPath: "./db", persistent: true))
                {
                    var client1 = embedded.Client;
                    var db1 = client1.GetDatabase("test");
                    var collection1 = db1.GetCollection<TestClass>("col");
                    var retrieved1 = await collection1.Find(x => x.Id == 12345).SingleOrDefaultAsync();
                    Assert.NotNull(retrieved1, "No object came back from the database.");
                    Assert.AreEqual("Hello world.", retrieved1.TestValue, "Unexpected test value came back.");

                }
            }

            finally
            {
                using (var embedded = new HostedMongoDbServer(dbPath: "./db", persistent: false))
                {
                }
            }

        }
    }
}
