using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Masticore.Storage
{
    /// <summary>
    /// IFileService implemented over Azure Storage
    /// </summary>
    public class StorageFileService<FileType> : BlobStorageBase, IFileService<FileType>
        where FileType : class, IFile
    {
        /// <summary>
        /// Extracts the container name from the given model
        /// By default, this reads the ContainerName property
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <returns></returns>
        protected virtual string ExtractContainerName(FileType cloudFile)
        {
            return cloudFile.ContainerName;
        }

        /// <summary>
        /// Extracts the filename from the given model
        /// By default, this reads the FileName property
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <returns></returns>
        protected virtual string ExtractBlobName(FileType cloudFile)
        {
            return cloudFile.FileName;
        }

        /// <summary>
        /// Gets the file URL for the given CloudFile
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <returns></returns>
        public virtual string GetFileUrl(FileType cloudFile)
        {
            string blobName = ExtractBlobName(cloudFile);
            string containerName = ExtractContainerName(cloudFile);
            return GetFileUrl(containerName, blobName);
        }

        /// <summary>
        /// Performs the asynchronous download from the given container and filename
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="fileName"></param>
        /// <returns></returns>
        protected virtual async Task<Stream> DownloadAsync(string containerName, string fileName)
        {
            CloudBlobContainer container = await GetContainerAsync(containerName);

            // If we don't have the blob, then just return null
            if (!(await container.HasBlobAsync(fileName)))
                return null;

            MemoryStream stream = new MemoryStream();
            await container.DownloadBlobAsync(fileName, stream);
            stream.Position = 0;
            return stream;
        }

        #region IFileService Implementation

        /// <summary>
        /// Uploads the given file asynchronously
        /// WARNING: If the file exists, this will overwrite it
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <param name="fileStream"></param>
        /// <returns></returns>
        public virtual async Task UploadAsync(FileType cloudFile, Stream fileStream)
        {
            if (cloudFile == null)
                throw new ArgumentNullException(nameof(cloudFile));

            if (fileStream == null)
                throw new ArgumentNullException(nameof(fileStream));

            string containerName = ExtractContainerName(cloudFile);
            string fileName = ExtractBlobName(cloudFile);
            CloudBlobContainer container = await GetContainerAsync(containerName);
            await container.UploadBlobAsync(fileName, fileStream);
        }

        /// <summary>
        /// Downloads the given cloudfile asynchronously
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <returns></returns>
        public virtual async Task<Stream> DownloadAsync(FileType cloudFile)
        {
            if (cloudFile == null)
                throw new ArgumentNullException(nameof(cloudFile));

            string containerName = ExtractContainerName(cloudFile);
            string fileName = ExtractBlobName(cloudFile);
            return await DownloadAsync(containerName, fileName);
        }

        /// <summary>
        /// Deletes the given cloud file asynchronously
        /// </summary>
        /// <param name="cloudFile"></param>
        /// <returns></returns>
        public virtual async Task DeleteAsync(FileType cloudFile)
        {
            if (cloudFile == null)
                throw new ArgumentNullException(nameof(cloudFile));

            string containerName = ExtractContainerName(cloudFile);
            CloudBlobContainer container = await GetContainerAsync(containerName);
            await container.DeleteBlobAsync(cloudFile.FileName);
        }

        #endregion
    }
}
