using Microsoft.VisualStudio.TestTools.UnitTesting;
using Masticore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Masticore.Storage.Tests
{
    [TestClass()]
    public class CloudTableFactoryTests
    {
        private const string TestTableName = "cloudtablefactortests";

        [TestMethod()]
        public async Task GetTableAsyncTest()
        {
            // Arrange
            CloudTableFactory factory = new CloudTableFactory();

            // Act
            var table = await factory.GetTableAsync(TestTableName);

            // Assert
            Assert.IsTrue(table.Exists());
        }

        [TestMethod()]
        public async Task DeleteTableAsyncTest()
        {
            // Arrange
            CloudTableFactory factory = new CloudTableFactory();

            // Act
            var table = await factory.GetTableAsync(TestTableName);
            await factory.DeleteTableAsync(TestTableName);

            // Assert
            Assert.IsFalse(table.Exists());
        }
    }
}