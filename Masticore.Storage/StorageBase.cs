using Microsoft.WindowsAzure.Storage;
using System;
using System.Configuration;

namespace Masticore.Storage
{
    /// <summary>
    /// A base class for managing azure storage
    /// This implements controlling the connection string and loading it from the app settings
    /// </summary>
    public abstract class StorageBase : INeedStorageConnectionString
    {
        /// <summary>
        /// Gets or sets the name of the connection string for connecting to storage
        /// </summary>
        public string ConnectionStringName { get; set; } = "StorageConnectionString";

        string _connectionString;

        bool? _useDevelopmentStorage;

        /// <summary>
        /// Gets the value in the AppSettings for BlobStorageBase.UseDevelopmentStorage
        /// This determines the strategy for building file URLs in GetFileUrl and the destination for files
        /// BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;
        /// TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;
        /// QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1; 
        /// </summary>
        protected bool UseDevelopmentStorage
        {
            get
            {
                if (_useDevelopmentStorage.HasValue) return _useDevelopmentStorage.Value;
                bool.TryParse(ConfigurationManager.AppSettings["BlobStorageBase.UseDevelopmentStorage"], out var val);
                _useDevelopmentStorage = val;
                return _useDevelopmentStorage.Value;
            }
        }

        /// <summary>
        /// Gets the connection string for this storage manager
        /// Child classes can customize the connection string by overriding this property
        /// </summary>
        public virtual string StorageConnectionString
        {
            get => _connectionString = _connectionString ?? FindConnectionString();
            set => _connectionString = value;
        }

        private string FindConnectionString()
        {
            // If it's dev, force the dev connection string
            if (UseDevelopmentStorage)
                return "UseDevelopmentStorage=true";

            if (ConnectionStringName == null)
                throw new ArgumentNullException(nameof(ConnectionStringName));

            var connString = ConfigurationManager.ConnectionStrings[ConnectionStringName];
            return connString?.ConnectionString ?? GetStorageConnectionString?.Invoke();
        }

        CloudStorageAccount _account;

        /// <summary>
        /// Gets the cloud storage account for this storage manager, loading it using the ConnectionString property
        /// </summary>
        protected virtual CloudStorageAccount Account => _account = _account ?? CloudStorageAccount.Parse(StorageConnectionString);

        protected string AccountName => Account.Credentials.AccountName;
        public Func<string> GetStorageConnectionString { get; set; }
    }
}
