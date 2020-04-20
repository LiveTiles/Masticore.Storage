using System;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Masticore.Storage
{
    /// <summary>
    /// IStorageCrud for any class implementing the ITableEntity interface (or children of TableEntity)
    /// </summary>
    /// <typeparam name="ModelType">Model type that must implement ITableEntity and have a public parameterless constructor</typeparam>
    public class TableEntityStorageCrud<ModelType> : IStorageTableCrud<ModelType>
        where ModelType : class, ITableEntity, new()
    {
        /// <summary>
        /// Gets or sets the table name underlying this repository
        /// </summary>
        public virtual string TableName { get; set; }

        /// <summary>
        /// Gets or sets the partition name underlying this repository
        /// </summary>
        public virtual string PartitionName { get; set; }

        protected IStorageTableFactory TableFactory { get; set; }

        /// <summary>
        /// Constructor that takes a table factory
        /// </summary>
        /// <param name="tableFactory"></param>
        public TableEntityStorageCrud(IStorageTableFactory tableFactory)
        {
            TableFactory = tableFactory;
        }

        /// <summary>
        /// Uses a CloudTableFactory to retrieve the table
        /// </summary>
        /// <returns></returns>
        protected virtual Task<CloudTable> GetTableAsync()
        {
            return TableFactory.GetTableAsync(TableName);
        }
        /// <summary>
        /// Choose the merge functionality for the existing and new entities (for the update).
        //TODO: Update MergeProperties to handle classes/objects that are not custom?
        /// </summary>
        /// <param name="existing">The entity that exists in table storage</param>
        /// <param name="newModel">The new entity that will be upserted into table storage</param>
        /// <returns>The merged object</returns>
        protected ModelType MergeEntities(ModelType existing, ModelType newModel)
        {
            //We have a special case for DynamicTableEntities....they do not use our custom attributes.
            if (existing.GetType().Name == newModel.GetType().Name && typeof(ITableEntity).IsAssignableFrom(typeof(ModelType)))
            {
                //Always use the new entity, but copy in the required identifiers
                newModel.PartitionKey = existing.PartitionKey;
                newModel.RowKey = existing.RowKey;
                newModel.ETag = existing.ETag;
                newModel.Timestamp = System.DateTimeOffset.UtcNow;
                
            }//Not sure we need this else?
            else
            {
                
                existing.MergeProperties(newModel, MergeMode.Update);
            }

            return newModel;
        }

        /// <summary>
        /// Inserts the given table entity model
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public virtual async Task<ModelType> CreateAsync(ModelType model)
        {
            // Write to the table
            var table = await GetTableAsync();
            model.PartitionKey = PartitionName;
            var result = await table.InsertEntityAsync(model);
            return (ModelType)result.Result;
        }

        /// <summary>
        /// Reads all table entity models from the current partition in the current table
        /// </summary>
        /// <returns></returns>
        public virtual async Task<IEnumerable<ModelType>> ReadAllAsync()
        {
            var table = await GetTableAsync();
            return await table.RetrieveEntitiesAsync<ModelType>(PartitionName);
        }

        /// <summary>
        /// Reads a single table entity model from the current partition in the current table
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public virtual async Task<ModelType> ReadAsync(string id)
        {
            var table = await GetTableAsync();
            return await table.RetrieveEntityAsync<ModelType>(PartitionName, id);
        }

        /// <summary>
        /// Inserts or replaces the given table entity model for the current partition in the current table
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public virtual async Task<ModelType> UpdateAsync(ModelType model)
        {
            var table = await GetTableAsync();
            var existingModel = await table.RetrieveEntityAsync<ModelType>(PartitionName, model.RowKey);

            //Either merge via class definition or use the ITableEntity
            var merge = MergeEntities(existingModel, model);
            merge.ETag = model.ETag ?? merge.ETag;

            var result = await table.ReplaceEntityAsync(merge);
            return (ModelType)result.Result;
        }

        /// <summary>
        /// Deletes the table entity model for the given id for the current partition in the current table
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public virtual async Task DeleteAsync(string id)
        {
            var table = await GetTableAsync();
            var tableEntity = await table.RetrieveEntityAsync<DynamicTableEntity>(PartitionName, id);
            await table.DeleteEntityAsync(tableEntity);
        }

        public Func<string> GetStorageConnectionString { get; set; }
    }
}
