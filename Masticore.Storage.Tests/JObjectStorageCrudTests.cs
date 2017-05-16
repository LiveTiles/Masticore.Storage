using Microsoft.VisualStudio.TestTools.UnitTesting;
using Masticore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            JObjectStorageCrud crud = await GetCrud();

            JObject obj = new JObject();
            obj["Name"] = "Erik";
            obj["Age"] = 32;

            // Act
            JObject createdObj = await crud.CreateAsync(obj);

            // Assert
            Assert.IsNotNull(createdObj);
            Assert.AreEqual(createdObj["Name"], "Erik");
            Assert.AreEqual(createdObj["Age"], 32);
        }

        [TestMethod()]
        public async Task ReadAllAsyncTest()
        {
            // Arrange
            JObjectStorageCrud crud = await GetCrud();
            await crud.CreateAsync(new JObject());
            await crud.CreateAsync(new JObject());
            await crud.CreateAsync(new JObject());

            // Act
            IEnumerable<JObject> collection = await crud.ReadAllAsync();

            // Assert
            Assert.IsTrue(collection.Count() == 3);
        }

        [TestMethod()]
        public async Task ReadAsyncTest()
        {
            // Arrange
            JObjectStorageCrud crud = await GetCrud();
            JObject obj = new JObject();
            JObject createdObj = await crud.CreateAsync(obj);

            // Act
            JObject readObj = await crud.ReadAsync(createdObj["Id"].ToString());

            // Assert
            Assert.IsNotNull(readObj);
        }

        [TestMethod()]
        public async Task UpdateAsyncTest()
        {
            // Arrange
            JObjectStorageCrud crud = await GetCrud();
            JObject obj = new JObject();
            obj["Name"] = "Erik";
            JObject createdObj = await crud.CreateAsync(obj);

            // Act
            createdObj["Name"] = "Jon";
            JObject updatedObj = await crud.UpdateAsync(createdObj);

            // Assert
            Assert.AreEqual(createdObj["Name"], updatedObj["Name"]);
        }

        [TestMethod()]
        public async Task DeleteAsyncTest()
        {
            // Arrange
            JObjectStorageCrud crud = await GetCrud();
            JObject obj = new JObject();
            JObject createdObj = await crud.CreateAsync(obj);

            // Act
            string id = createdObj["Id"].ToString();
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

        #region Support Methods

        private static async Task<JObjectStorageCrud> GetCrud()
        {
            CloudTableFactory tableFactory = new CloudTableFactory();
            await tableFactory.DeleteTableAsync(TableName);
            TableEntityStorageCrud<DynamicTableEntity> innerCrud = new TableEntityStorageCrud<DynamicTableEntity>(tableFactory);
            JObjectStorageCrud crud = new JObjectStorageCrud(innerCrud);
            crud.TableName = TableName;
            crud.PartitionName = "Ralston";
            return crud;
        }

        #endregion
    }
}
