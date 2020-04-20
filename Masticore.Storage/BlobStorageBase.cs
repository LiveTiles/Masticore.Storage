using System;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace Masticore.Storage
{
    /// <summary>
    /// A general-purpose base class for using the CloudBlobClient and caching containers in-object
    /// </summary>
    public abstract class BlobStorageBase : StorageBase
    {
        /// <summary>
        /// In-object cache of containers - not thread safe
        /// </summary>
        readonly Dictionary<string, CloudBlobContainer> _containerCache = new Dictionary<string, CloudBlobContainer>();

        /// <summary>
        /// Blob client instance
        /// </summary>
        CloudBlobClient _blobClient;
        

        /// <summary>
        /// Builds the URL for the given accountName and container name
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        protected virtual string GetFileUrl(string containerName, string blobName)
        {
            var sanitizedBlobName = blobName
                .Replace("?", "%3F")
                .Replace("#", "%23");

            // Development can't use HTTPS, so keep it HTTP
            return UseDevelopmentStorage
                ? $"http://127.0.0.1:10000/{AccountName}/{containerName}/{sanitizedBlobName}"
                : $"//{AccountName}.blob.core.windows.net/{containerName}/{sanitizedBlobName}";
        }

        protected string GetSasToken(CloudBlob blob, int beginMinutes = -5,
            int endHours = 1, SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read)
        {
            return blob.GetSharedAccessSignature(new SharedAccessBlobPolicy
            {
                SharedAccessStartTime = DateTimeOffset.UtcNow.AddMinutes(beginMinutes),
                SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddHours(endHours),
                Permissions = permissions
            });
        }
        /// <summary>
        /// Get a Shared Access Signature Token for the given container and blob
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <param name="beginMinutes">Minutes added to current time for beginning of access window (default: -5 minutes)</param>
        /// <param name="endHours">Hours added to current time for end of access window (default: 1 hour)</param>
        /// <param name="permissions">One or more (via union) of permissions (default: read)</param>
        /// <returns></returns>
        protected async Task<string> GetSasToken(string containerName, string blobName, int beginMinutes = -5, int endHours = 1, SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read)
        {
            var container = await GetContainerAsync(containerName);
            var blob = container.GetBlobReference(blobName);
            return GetSasToken(blob, beginMinutes, endHours, permissions);
        }

        protected string AddSasToken(string fileUrl, int beginMinutes = -5, int endHours = 1, SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read)
        {
            var uri = new UriBuilder(fileUrl).Uri;
            var blob = new CloudBlob(uri, Account.Credentials);
            var token = GetSasToken(blob, beginMinutes, endHours, permissions);
            return fileUrl + token;
        }

        /// <summary>
        /// Gets a reference to the given container, creating it if it doesn't exist
        /// </summary>
        /// <param name="containerName"></param>
        /// <returns></returns>
        protected virtual async Task<CloudBlobContainer> GetContainerAsync(string containerName)
        {
            // If we already have a container by this name, return the cached version
            if (_containerCache.ContainsKey(containerName))
                return _containerCache[containerName];

            // Create the blob client only if necessary
            _blobClient = _blobClient ?? Account.CreateCloudBlobClient();

            //Set blob request options in order to chunk file uploads.
            var blobRequestOptions = GetBlobRequestOptions(1, 5, 1);

            if (blobRequestOptions != null)
                _blobClient.DefaultRequestOptions = blobRequestOptions;


            // Retrieve, create if not exists, and cache the container
            var container = _blobClient.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();
            _containerCache[containerName] = container;

            return container;
        }

        /// <summary>
        /// Set the upload parameters such as retry count, max megabytes for a single-blob upload.
        /// </summary>
        /// <param name="megabyte">The maximum megabytes for a single-blob upload</param>
        /// <param name="timespan">The backoff period in seconds</param>
        /// <param name="retryCount">The number of times to retry on failure</param>
        /// <returns>BlobRequestOptions</returns>
        protected virtual BlobRequestOptions GetBlobRequestOptions(byte megabyte = 1, byte timespan = 5, byte retryCount = 1)
        {
            try
            {
                if (megabyte < 1 || megabyte > 5 || timespan > 60 || retryCount > 10)
                    throw new ArgumentException(string.Format("Invalid arguments for GetBlobRequestOptions. {0}, {1}, {2}", megabyte, timespan, retryCount));

                TimeSpan backOffPeriod = TimeSpan.FromSeconds(timespan);

                BlobRequestOptions bro = new BlobRequestOptions()
                {
                    SingleBlobUploadThresholdInBytes = 1024 * 1024 * megabyte, 
                    ParallelOperationThreadCount = 1,
                    RetryPolicy = new ExponentialRetry(backOffPeriod, retryCount),
                };
                return bro;
            }
            catch(Exception)
            {
             
                return null;
            }
        }
    }
}
