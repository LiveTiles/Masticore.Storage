using Microsoft.VisualStudio.TestTools.UnitTesting;
using Masticore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Masticore.Storage.Tests
{
    public class TestEntity : TableEntity
    {
        public string Name { get; set; }
    }

    [TestClass()]
    public class TableEntityStorageCrudTests
    {
        private const string TableName = "People";

        [TestMethod()]
        public async Task CreateAsyncTest()
        {
            // Arrange
            CloudTableFactory factory = new CloudTableFactory();
            await factory.DeleteTableAsync(TableName);

            TableEntityStorageCrud<TestEntity> crud = new TableEntityStorageCrud<TestEntity>(factory);
            crud.TableName = TableName;
            crud.PartitionName = "Ralston";
            TestEntity testEnity = new TestEntity { Name = "Evelyn Ralston", PartitionKey = "Ralston", RowKey = "Evee" };

            // Act
            TestEntity createdEntity = await crud.CreateAsync(testEnity);

            // Assert
            Assert.AreEqual(testEnity.Name, createdEntity.Name);
        }

        [TestMethod()]
        public async Task ReadAllAsyncTest()
        {
            // Arrange
            CloudTableFactory factory = new CloudTableFactory();
            await factory.DeleteTableAsync(TableName);

            TableEntityStorageCrud<TestEntity> crud = new TableEntityStorageCrud<TestEntity>(factory);
            crud.TableName = TableName;
            crud.PartitionName = "Ralston";

            await crud.CreateAsync(new TestEntity { Name = "Erik Ralston", PartitionKey = "Ralston", RowKey = "Erik" });
            await crud.CreateAsync(new TestEntity { Name = "Evee Ralston", PartitionKey = "Ralston", RowKey = "Evelyn" });
            await crud.CreateAsync(new TestEntity { Name = "Lilly Ralston", PartitionKey = "Ralston", RowKey = "Lillian" });

            // Act
            IEnumerable<TestEntity> entities = await crud.ReadAllAsync();

            // Assert
            Assert.IsTrue(entities.Count() == 3);
        }

        [TestMethod()]
        public async Task ReadAsyncTest()
        {
            // Arrange
            CloudTableFactory factory = new CloudTableFactory();
            await factory.DeleteTableAsync(TableName);

            TableEntityStorageCrud<TestEntity> crud = new TableEntityStorageCrud<TestEntity>(factory);
            crud.TableName = TableName;
            crud.PartitionName = "Ralston";
            TestEntity testEnity = new TestEntity { Name = "Evelyn Ralston", PartitionKey = "Ralston", RowKey = "Evee" };
            TestEntity createdEntity = await crud.CreateAsync(testEnity);

            // Act
            TestEntity readEntity = await crud.ReadAsync("Evee");

            // Assert
            Assert.IsNotNull(readEntity);
            Assert.AreEqual(testEnity.Name, createdEntity.Name);
            Assert.AreEqual(testEnity.Name, readEntity.Name);
        }

        [TestMethod()]
        public async Task UpdateAsyncTest()
        {
            // Arrange
            CloudTableFactory factory = new CloudTableFactory();
            await factory.DeleteTableAsync(TableName);

            TableEntityStorageCrud<TestEntity> crud = new TableEntityStorageCrud<TestEntity>(factory);
            crud.TableName = TableName;
            crud.PartitionName = "Ralston";
            TestEntity testEntity = new TestEntity { Name = "Evelyn Ralston", PartitionKey = "Ralston", RowKey = "Evee" };
            TestEntity createdEntity = await crud.CreateAsync(testEntity);

            // Act
            createdEntity.Name = "Evelyn";
            TestEntity updatedEntity = await crud.UpdateAsync(createdEntity);


            // Assert
            TestEntity readEntity = await crud.ReadAsync("Evee");
            Assert.IsNotNull(updatedEntity);
            Assert.AreEqual(createdEntity.Name, readEntity.Name);
        }

        [TestMethod()]
        public async Task DeleteAsyncTest()
        {
            // Arrange
            CloudTableFactory factory = new CloudTableFactory();
            await factory.DeleteTableAsync(TableName);

            TableEntityStorageCrud<TestEntity> crud = new TableEntityStorageCrud<TestEntity>(factory);
            crud.TableName = "People";
            crud.PartitionName = "Ralston";
            TestEntity testEnity = new TestEntity { Name = "Evelyn Ralston", PartitionKey = "Ralston", RowKey = "Evee" };
            TestEntity createdEntity = await crud.CreateAsync(testEnity);

            // Act
            await crud.DeleteAsync("Evee");

            // Assert
            TestEntity readEntity = await crud.ReadAsync("Evee");
            Assert.IsNull(readEntity);
        }
    }
}