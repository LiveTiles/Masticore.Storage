using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Masticore.Storage
{
    /// <summary>
    /// A class with extensions to make working with tables easier
    /// Includes logging for all methods that change the table
    /// </summary>
    public static class CloudTableExtensions
    {
        /// <summary>
        /// Asynchronously inserts the given entity into the table
        /// </summary>
        /// <param name="table"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static Task<TableResult> InsertEntityAsync(this CloudTable table, ITableEntity entity)
        {
            System.Diagnostics.Trace.TraceInformation("Asynchronously inserting entity into azure table '{0}' with partition '{1}' and row '{2}'", table.Name, entity.PartitionKey, entity.RowKey);

            // Create the TableOperation that inserts the customer entity.
            var insertOperation = TableOperation.Insert(entity);

            // Execute the operation asynchronously
            return table.ExecuteAsync(insertOperation);
        }

        /// <summary>
        /// Inserts or replaces the given entity in the table
        /// </summary>
        /// <param name="table"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static Task<TableResult> InsertOrReplaceEntityAsync(this CloudTable table, ITableEntity entity)
        {
            System.Diagnostics.Trace.TraceInformation("Asynchronously inserting or replacing entity into azure table '{0}' with partition '{1}' and row '{2}'", table.Name, entity.PartitionKey, entity.RowKey);

            // Create the InsertOrReplace TableOperation
            var insertOrReplaceOperation = TableOperation.InsertOrReplace(entity);

            // Execute the operation asynchronously
            return table.ExecuteAsync(insertOrReplaceOperation);
        }

        /// <summary>
        /// Replaces the given entity in the table
        /// </summary>
        /// <param name="table"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static Task<TableResult> ReplaceEntityAsync(this CloudTable table, ITableEntity entity)
        {
            System.Diagnostics.Trace.TraceInformation("Asynchronously replacing entity into azure table '{0}' with partition '{1}' and row '{2}'", table.Name, entity.PartitionKey, entity.RowKey);

            // Create the InsertOrReplace TableOperation
            var replaceOperation = TableOperation.Replace(entity);

            // Execute the operation asynchronously
            return table.ExecuteAsync(replaceOperation);
        }

        /// <summary>
        /// Asynchronously delete the given entity from the given table
        /// </summary>
        /// <param name="table"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static Task<TableResult> DeleteEntityAsync(this CloudTable table, ITableEntity entity)
        {
            System.Diagnostics.Trace.TraceInformation("Asynchronously deleting entity from azure table '{0}' with partition '{1}' and row '{2}'", table.Name, entity.PartitionKey, entity.RowKey);

            // Create the Delete TableOperation
            var deleteOperation = TableOperation.Delete(entity);

            // Execute the operation asynchronously
            return table.ExecuteAsync(deleteOperation);
        }

        /// <summary>
        /// Retrieves a single entity with the given partition and rowKey
        /// NOTE: This will return null if the entity was not found
        /// </summary>
        /// <typeparam name="EntityType"></typeparam>
        /// <param name="table"></param>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        public static async Task<EntityType> RetrieveEntityAsync<EntityType>(this CloudTable table, string partitionKey, string rowKey) where EntityType : ITableEntity
        {
            System.Diagnostics.Trace.TraceInformation("Asynchronously retrieving entity from azure table '{0}' with partition '{1}' and row '{2}'", table.Name, partitionKey, rowKey);

            // Setup the retrieve
            var retrieveOperation = TableOperation.Retrieve<EntityType>(partitionKey, rowKey);

            // Execute the operation async
            var retrievedResult = await table.ExecuteAsync(retrieveOperation);

            var entity = (EntityType)retrievedResult.Result;

            return entity;
        }

        /// <summary>
        /// Extension method for executing queries aynsynchonously against a CloudTable
        /// </summary>
        /// <typeparam name="TableEntityType"></typeparam>
        /// <param name="table"></param>
        /// <param name="query"></param>
        /// <param name="takeLimit"></param>
        /// <param name="ct"></param>
        /// <param name="onProgress"></param>
        /// <returns></returns>
        public static async Task<IList<TableEntityType>> ExecuteQueryAsync<TableEntityType>(this CloudTable table, TableQuery<TableEntityType> query, int? takeLimit = null, CancellationToken ct = default(CancellationToken), Action<IList<TableEntityType>> onProgress = null) where TableEntityType : ITableEntity, new()
        {
            System.Diagnostics.Trace.TraceInformation("Executing query for azure table '{0}'", table.Name);

            var items = new List<TableEntityType>();
            TableContinuationToken token = null;

            do
            {
                var seg = await table.ExecuteQuerySegmentedAsync<TableEntityType>(query, token);
                token = seg.ContinuationToken;
                items.AddRange(seg);
                if (onProgress != null)
                    onProgress(items);

                if (takeLimit.HasValue && items.Count >= takeLimit.Value)
                    break;

            } while (token != null && !ct.IsCancellationRequested);

            return items;
        }

        /// <summary>
        /// Retrieves all entities in the given partition from the given table, optionally taking only the top number of entities
        /// </summary>
        /// <typeparam name="EntityType"></typeparam>
        /// <param name="table"></param>
        /// <param name="partitionKey"></param>
        /// <param name="takeLimit"></param>
        /// <returns></returns>
        public static Task<IList<EntityType>> RetrieveEntitiesAsync<EntityType>(this CloudTable table, string partitionKey, int? takeLimit = null) where EntityType : ITableEntity, new()
        {
            System.Diagnostics.Trace.TraceInformation("Retrieving all entities from azure table '{0}' in partition '{1}'", table.Name, partitionKey);

            var query = new TableQuery<EntityType>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            if (takeLimit.HasValue)
                query = query.Take(takeLimit.Value);

            return table.ExecuteQueryAsync<EntityType>(query, takeLimit);
        }

        /// <summary>
        /// Retrieves all entities in the given table, optionally taking only the top number of entities
        /// </summary>
        /// <typeparam name="EntityType"></typeparam>
        /// <param name="table"></param>
        /// <param name="takeLimit"></param>
        /// <returns></returns>
        public static Task<IList<EntityType>> RetrieveEntitiesAsync<EntityType>(this CloudTable table, int? takeLimit = null) where EntityType : ITableEntity, new()
        {
            System.Diagnostics.Trace.TraceInformation("Retrieving all entities from azure table '{0}'", table.Name);

            var query = new TableQuery<EntityType>();

            if (takeLimit.HasValue)
                query = query.Take(takeLimit.Value);

            return table.ExecuteQueryAsync<EntityType>(query, takeLimit);
        }
    }
}
