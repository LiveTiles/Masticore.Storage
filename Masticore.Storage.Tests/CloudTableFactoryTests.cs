using Microsoft.VisualStudio.TestTools.UnitTesting;
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
            var factory = new CloudTableFactory();

            // Act
            var table = await factory.GetTableAsync(TestTableName);

            // Assert
            Assert.IsTrue(table.Exists());
        }

        [TestMethod()]
        public async Task DeleteTableAsyncTest()
        {
            // Arrange
            var factory = new CloudTableFactory();

            // Act
            var table = await factory.GetTableAsync(TestTableName);
            await factory.DeleteTableAsync(TestTableName);

            // Assert
            Assert.IsFalse(table.Exists());
        }
    }
}