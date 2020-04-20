using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Masticore.Storage
{
    /// <summary>
    /// Interface for an object that can get (creating if necessary) and delete tables
    /// </summary>
    public interface IStorageTableFactory : INeedStorageConnectionString
    {
        string StorageConnectionString { get; set; }
        /// <summary>
        /// Async deletes the given table, destroying all data
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        Task DeleteTableAsync(string tableName);

        /// <summary>
        /// Gets a live reference to this table, ensuring the instance exists
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        Task<CloudTable> GetTableAsync(string tableName);
    }

    /// <summary>
    /// ICloudTableFactory with caching and a naive strategy for ensuring all returned tables exist
    /// Built for easy sub-classing to drive async CloudTableClient instantiation and CloudTable create behavior 
    /// </summary>
    public class CloudTableFactory : StorageBase, IStorageTableFactory
    {
        /// <summary>
        /// Persistent instance of the table client for this object, lazy loaded
        /// </summary>
        CloudTableClient _tableClient = null;

        /// <summary>
        /// A map of tables, to ensure we are not hitting the cloud table check too many times
        /// </summary>
        Dictionary<string, CloudTable> _tables = new Dictionary<string, CloudTable>();

        /// <summary>
        /// Asynchronously retrieves the table client
        /// Child classes can override this to control tableclient life-cycle
        /// </summary>
        /// <returns></returns>
        protected virtual Task<CloudTableClient> GetTableClientAsync()
        {
            _tableClient = _tableClient ?? Account.CreateCloudTableClient();
            return Task.FromResult(_tableClient);
        }

        /// <summary>
        /// Asynchronously creates the table in Azure if it doesn't exist yet
        /// Child classes can override this to control table creation life-cycle
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        protected virtual async Task CreateIfNotExistsAsync(CloudTable table)
        {
            // Create if not already there
            await table.CreateIfNotExistsAsync();
        }

        /// <summary>
        /// Gets a table reference for the given table name, creating if not found by default
        /// Child classes can override this to change behavior like how it handles creating if not found
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        protected virtual async Task<CloudTable> GetTableReferenceAsync(string tableName)
        {
            // Pull the client async
            var client = await GetTableClientAsync();
            // Get a reference to the table
            var table = client.GetTableReference(tableName);
            await CreateIfNotExistsAsync(table);
            return table;
        }

        /// <summary>
        /// Asynchronously retrieves a reference to the table with the given table name, using a cache to retrieve if already exists
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public async Task<CloudTable> GetTableAsync(string tableName)
        {
            // Check table cache and return if found
            if (_tables.ContainsKey(tableName))
                return _tables[tableName];

            // Async create it, since it wasn't found
            var table = await GetTableReferenceAsync(tableName);

            // Save to cache
            _tables[tableName] = table;

            return table;
        }

        /// <summary>
        /// Asynchronously deletes the table with the given name, destroying all data
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public virtual async Task DeleteTableAsync(string tableName)
        {
            var cloudTable = await GetTableAsync(tableName);
            _tables.Remove(tableName);
            if (await cloudTable.ExistsAsync())
                await cloudTable.DeleteAsync();
        }
    }
}
