using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Masticore.Storage
{
    /// <summary>
    /// IFileService implemented over Azure Storage
    /// TODO: Ensure file has changed to save on storage/snapshot.
    /// </summary>
    public class StorageFileService<TFile> : BlobStorageBase, IFileService<TFile>
        where TFile : class, IFile
    {
        /// <summary>
        /// Deletes the given cloud file asynchronously
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <returns></returns>
        public virtual async Task DeleteAsync(TFile cloudFile)
        {
            if (cloudFile == null)
                throw new ArgumentNullException(nameof(cloudFile));

            var containerName = ExtractContainerName(cloudFile);
            var fileName = ExtractBlobName(cloudFile);
            var container = await GetContainerAsync(containerName);
            await container.DeleteBlobAsync(fileName);
        }

        public virtual async Task DeleteAsync(TFile cloudFile, bool? isIncludeSnapshot)
        {
            if (cloudFile == null)
                throw new ArgumentNullException(nameof(cloudFile));

            var containerName = ExtractContainerName(cloudFile);
            var fileName = ExtractBlobName(cloudFile);
            var container = await GetContainerAsync(containerName);
            await container.DeleteBlobAsync(fileName);
        }

        /// <summary>
        /// Downloads the given cloudfile asynchronously
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <returns></returns>
        public virtual async Task<Stream> DownloadAsync(TFile cloudFile)
        {
            if (cloudFile == null)
                throw new ArgumentNullException(nameof(cloudFile));

            var containerName = ExtractContainerName(cloudFile);
            var fileName = ExtractBlobName(cloudFile);
            return await DownloadAsync(containerName, fileName);
        }

        /// <summary>
        /// Lists all the <see cref="TFile"/>s within an folder
        /// </summary>
        /// <param name="containerName">The Azure Container Name</param>
        /// <param name="path">The directory within the container</param>
        /// <returns></returns>
        public virtual async Task<IEnumerable<string>> GetAllFileNames(string containerName, string path)
        {
            var container = await GetContainerAsync(containerName);
            var directory = container.GetDirectoryReference(path);
            var list = directory.ListBlobs().OfType<Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob>().Select(item => item.Name.Split('/').Last());

            return list;
        }

        /// <summary>
        /// Gets the file URL for the given CloudFile
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <returns></returns>
        public virtual string GetFileUrl(TFile cloudFile)
        {
            var blobName = ExtractBlobName(cloudFile);
            var containerName = ExtractContainerName(cloudFile);
            return GetFileUrl(containerName, blobName);
        }

        public async Task<string> GetFileUrlWithToken(TFile cloudFile)
        {
            var blobName = ExtractBlobName(cloudFile);
            var containerName = ExtractContainerName(cloudFile);
            var token = await GetSasToken(containerName, blobName);
            return GetFileUrl(cloudFile) + token;
        }

        public string GetFileUrlWithToken(string fileUrl)
        {
            return AddSasToken(fileUrl);
        }

        /// <summary>
        /// Creates a snapshot of a given cloudFile.
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <returns></returns>
        public virtual async Task SnapshotAsync(TFile cloudFile)
        {
            if (cloudFile == null)
                throw new ArgumentNullException(nameof(cloudFile));

            var containerName = ExtractContainerName(cloudFile);
            var fileName = ExtractBlobName(cloudFile);
            var container = await GetContainerAsync(containerName);

            await container.SnapshotBlobAsync(fileName);
        }

        /// <summary>
        /// Uploads the given file asynchronously
        /// WARNING: If the file exists, this will overwrite it
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <param name="fileStream"></param>
        /// <returns></returns>
        public virtual async Task UploadAsync(TFile cloudFile, Stream fileStream)
        {
            if (cloudFile == null)
                throw new ArgumentNullException(nameof(cloudFile));

            if (fileStream == null)
                throw new ArgumentNullException(nameof(fileStream));

            var containerName = ExtractContainerName(cloudFile);
            var fileName = ExtractBlobName(cloudFile);
            var container = await GetContainerAsync(containerName);
            await container.UploadBlobAsync(fileName, fileStream);
        }

        public virtual Task UploadAsync(TFile cloudFile, string fileContent)
        {
            return UploadAsync(cloudFile, fileContent.ToStream());
        }

        /// <summary>
        /// Uploads the given file asynchronously, optionally creating a snapshot.
        /// WARNING: If the file exists, this will overwrite it
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <param name="fileStream"></param>
        /// <param name="isSnapshot"></param>
        /// <returns></returns>
        public virtual async Task UploadAsync(TFile cloudFile, Stream fileStream, bool isSnapshot = false)
        {

            await UploadAsync(cloudFile, fileStream);
            if (isSnapshot)
            {
                await SnapshotAsync(cloudFile);
            }
        }

        /// <summary>
        /// Uploads the given file asynchronously, optionally creating a snapshot.
        /// WARNING: If the file exists, this will overwrite it
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <param name="fileContent"></param>
        /// <param name="isSnapshot"></param>
        /// <returns></returns>
        public virtual async Task UploadAsync(TFile cloudFile, string fileContent, bool isSnapshot = false)
        {

            await UploadAsync(cloudFile, fileContent.ToStream());
            if (isSnapshot)
            {
                await SnapshotAsync(cloudFile);
            }
        }

        /// <summary>
        /// Performs the asynchronous download from the given container and filename
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="fileName"></param>
        /// <returns></returns>
        protected virtual async Task<Stream> DownloadAsync(string containerName, string fileName)
        {
            var container = await GetContainerAsync(containerName);

            // If we don't have the blob, then just return null
            if (!(await container.HasBlobAsync(fileName)))
                return null;

            var stream = new MemoryStream();
            await container.DownloadBlobAsync(fileName, stream);
            stream.Position = 0;
            return stream;
        }

        /// <summary>
        /// Extracts the filename from the given model
        /// By default, this reads the FileName property
        /// </summary>
        /// <param name="pageEntity"></param>
        /// <returns></returns>
        protected virtual string ExtractBlobName(TFile pageEntity)
        {
            return pageEntity.FileName;
        }

        /// <summary>
        /// Extracts the container name from the given model
        /// By default, this reads the ContainerName property
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <returns></returns>
        protected virtual string ExtractContainerName(TFile cloudFile)
        {
            return cloudFile.ContainerName;
        }
    }
}
