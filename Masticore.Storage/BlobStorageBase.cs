using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Masticore.Storage
{
    /// <summary>
    /// A general-purpose base class for using the CloudBlobClient and caching containers in-object
    /// </summary>
    public abstract class BlobStorageBase : StorageBase
    {
        #region Fields

        /// <summary>
        /// In-object cache of containers - not thread safe
        /// </summary>
        readonly Dictionary<string, CloudBlobContainer> _containerCache = new Dictionary<string, CloudBlobContainer>();

        /// <summary>
        /// Blob client instance
        /// </summary>
        CloudBlobClient _blobClient;

        #endregion

        #region Methods


        /// <summary>
        /// Builds the URL for the given accountName and container name
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        protected virtual string GetFileUrl(string containerName, string blobName)
        {
            string accountName = Account.Credentials.AccountName;

            // Development can't use HTTPS, so keep it HTTP
            if (UseDevelopmentStorage)
                return string.Format("http://127.0.0.1:10000/{0}/{1}/{2}", accountName, containerName, blobName);
            else
                return string.Format("//{0}.blob.core.windows.net/{1}/{2}", accountName, containerName, blobName);
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

            // Retrieve, create if not exists, and cache the container
            CloudBlobContainer container = _blobClient.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();
            _containerCache[containerName] = container;

            return container;
        }

        #endregion
    }
}
