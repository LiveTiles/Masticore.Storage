using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading.Tasks;
using System.IO;

namespace Masticore.Storage.Tests
{
    public class TestFile : IFile
    {
        public string ContainerName
        {
            get
            {
                return "testfilecontainername";
            }
        }

        public string FileName
        {
            get
            {
                return "testfilefilename";
            }
        }
    }

    [TestClass()]
    public class StorageFileServiceTests
    {
        [TestMethod()]
        public void GetFileUrlTest()
        {
            // Arrange
            var fileService = new StorageFileService<TestFile>();
            var file = new TestFile();

            // Act
            var name = fileService.GetFileUrl(file);

            // Assert
            Assert.IsNotNull(name);
            Assert.IsTrue(name.StartsWith("http://127.0.0.1:10000/", StringComparison.CurrentCulture));

        }

        [TestMethod()]
        public async Task UploadAsyncTest()
        {
            // Arrange
            var fileService = new StorageFileService<TestFile>();
            var file = new TestFile();
            var stream = LoadFileStream();

            // Act
            await fileService.UploadAsync(file, stream);

            // Assert - success is passing
        }

        [TestMethod()]
        public async Task DownloadAsyncTest()
        {
            // Arrange
            var fileService = new StorageFileService<TestFile>();
            var file = new TestFile();
            var stream = LoadFileStream();
            await fileService.UploadAsync(file, stream);

            // Act
            var readStream = await fileService.DownloadAsync(file);

            // Assert
            Assert.IsTrue(stream.Length == readStream.Length && readStream.Length > 0);
        }

        [TestMethod()]
        public async Task DeleteAsyncTest()
        {
            // Arrange - Load file and ensure existence
            var fileService = new StorageFileService<TestFile>();
            var file = new TestFile();
            var stream = LoadFileStream();
            await fileService.UploadAsync(file, stream);
            var readStream = await fileService.DownloadAsync(file);
            Assert.IsTrue(stream.Length == readStream.Length && readStream.Length > 0);

            // Act
            await fileService.DeleteAsync(file);

            // Assert
            var assertStream = await fileService.DownloadAsync(file);
            Assert.IsNull(assertStream);
        }


        private static Stream LoadFileStream()
        {
            var path = Path.Combine(Environment.CurrentDirectory, "../../Geordi.jpg");
            Stream stream = File.OpenRead(path);
            return stream;
        }
    }
}