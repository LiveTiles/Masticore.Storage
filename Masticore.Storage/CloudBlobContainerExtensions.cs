using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Threading.Tasks;

namespace Masticore.Storage
{
    /// <summary>
    /// Simple extensions for CloudBlobContainer, the managing object of table stores
    /// Includes logging for upload and download actions
    /// </summary>
    public static class CloudBlobContainerExtensions
    {
        /// <summary>
        /// Asynchronously deletes the block with the given name in the given bucket
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public static Task DeleteBlobAsync(this CloudBlobContainer container, string blobName)
        {
            System.Diagnostics.Trace.TraceInformation("Deleting azure storage blob '{0}' from container '{1}'", blobName, container.Name);
            var blockBlob = container.GetBlockBlobReference(blobName);
            return blockBlob.DeleteIfExistsAsync();
        }

        /// <summary>
        /// Pulls the reference for the given blob and downloads it asynchronously to the given stream
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobName"></param>
        /// <param name="downloadStream"></param>
        /// <returns></returns>
        public static Task DownloadBlobAsync(this CloudBlobContainer container, string blobName, Stream downloadStream)
        {
            System.Diagnostics.Trace.TraceInformation("Downloading azure storage blob '{0}' from container '{1}'", blobName, container.Name);
            var blockBlob = container.GetBlockBlobReference(blobName);
            blockBlob.Properties.ContentType = System.Web.MimeMapping.GetMimeMapping(blobName);
            return blockBlob.DownloadToStreamAsync(downloadStream);
        }

        public static string GetContentType(string fileName)
        {
            string contentType = System.Web.MimeMapping.GetMimeMapping(fileName);

            string charset = "";

            switch (contentType)
            {
                case "application/x-javascript":
                case "text/html":
                    charset = "; charset=utf-8";
                    break;

                default:
                    break;
            }

            return contentType + charset;
        }

        /// <summary>
        /// Asynchronously checks if the given filename exists in the containers
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public static Task<bool> HasBlobAsync(this CloudBlobContainer container, string blobName)
        {
            System.Diagnostics.Trace.TraceInformation("Checking if azure storage blob '{0}' exists in container '{1}'", blobName, container.Name);
            var blockBlob = container.GetBlockBlobReference(blobName);
            return blockBlob.ExistsAsync();
        }

        /// <summary>
        /// Creates a snapshot of the given block blob.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public static Task SnapshotBlobAsync(this CloudBlobContainer container, string blobName)
        {
            var blockBlob = container.GetBlockBlobReference(blobName);

            if (blockBlob.Exists())
            {
                return blockBlob.CreateSnapshotAsync();
            }
            return null;
        }

        /// <summary>
        /// Pulls the reference for the given blob and uploads the stream to it asynchronously
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobName"></param>
        /// <param name="uploadStream"></param>
        /// <returns></returns>
        ///TODO: Make the StreamWriteSizeInBytes be configurable
        public static Task UploadBlobAsync(this CloudBlobContainer container, string blobName, Stream uploadStream)
        {
            System.Diagnostics.Trace.TraceInformation("Uploading azure storage blob '{0}' to container '{1}'", blobName, container.Name);
            var blockBlob = container.GetBlockBlobReference(blobName);
            blockBlob.Properties.ContentType = GetContentType(blobName);
            blockBlob.StreamWriteSizeInBytes = 500 * 1024; //500 KB
            return blockBlob.UploadFromStreamAsync(uploadStream);
        }
    }
}
