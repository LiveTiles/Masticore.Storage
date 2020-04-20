using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Microsoft.WindowsAzure.Storage.Table;

namespace Masticore.Storage.Tests
{
    [TestClass()]
    public class JObjectStorageCrudTests
    {
        public const string TableName = "Humans";

        [TestMethod()]
        public async Task CreateAsyncTest()
        {
            // Arrange
            var crud = await GetCrud();

            var obj = new JObject();
            obj["Name"] = "Erik";
            obj["Age"] = 32;

            // Act
            var createdObj = await crud.CreateAsync(obj);

            // Assert
            Assert.IsNotNull(createdObj);
            Assert.AreEqual(createdObj["Name"], "Erik");
            Assert.AreEqual(createdObj["Age"], 32);
        }

        [TestMethod()]
        public async Task ReadAllAsyncTest()
        {
            // Arrange
            var crud = await GetCrud();
            await crud.CreateAsync(new JObject());
            await crud.CreateAsync(new JObject());
            await crud.CreateAsync(new JObject());

            // Act
            var collection = await crud.ReadAllAsync();

            // Assert
            Assert.IsTrue(collection.Count() == 3);
        }

        [TestMethod()]
        public async Task ReadAsyncTest()
        {
            // Arrange
            var crud = await GetCrud();
            var obj = new JObject();
            var createdObj = await crud.CreateAsync(obj);

            // Act
            var readObj = await crud.ReadAsync(createdObj["Id"].ToString());

            // Assert
            Assert.IsNotNull(readObj);
        }

        [TestMethod()]
        public async Task UpdateAsyncTest()
        {
            // Arrange
            var crud = await GetCrud();
            var obj = new JObject();
            obj["Name"] = "Erik";
            var createdObj = await crud.CreateAsync(obj);

            // Act
            createdObj["Name"] = "Jon";
            var updatedObj = await crud.UpdateAsync(createdObj);

            // Assert
            Assert.AreEqual(createdObj["Name"], updatedObj["Name"]);
        }

        [TestMethod()]
        public async Task DeleteAsyncTest()
        {
            // Arrange
            var crud = await GetCrud();
            var obj = new JObject();
            var createdObj = await crud.CreateAsync(obj);

            // Act
            var id = createdObj["Id"].ToString();
            await crud.DeleteAsync(id);

            // Assert
            try
            {
                await crud.ReadAsync(id);
                Assert.Fail();
            }
            catch (Exception exc)
            {
                Assert.IsTrue(exc.Message.Contains("cannot be found"));
            }
        }

        private static async Task<JObjectStorageCrud> GetCrud()
        {
            var tableFactory = new CloudTableFactory();
            await tableFactory.DeleteTableAsync(TableName);
            var innerCrud = new TableEntityStorageCrud<DynamicTableEntity>(tableFactory);
            var crud = new JObjectStorageCrud(innerCrud);
            crud.TableName = TableName;
            crud.PartitionName = "Ralston";
            return crud;
        }
    }
}
