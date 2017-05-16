using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Masticore.Storage
{
    /// <summary>
    /// IDynamicStorageCrud implemented with JObject from Newtonsoft JSON
    /// </summary>
    public class JObjectStorageCrud : IStorageTableCrud<JObject>
    {
        IStorageTableCrud<DynamicTableEntity> _dynamicStorage;

        public JObjectStorageCrud(IStorageTableCrud<DynamicTableEntity> dynamicStorage)
        {
            _dynamicStorage = dynamicStorage;
        }

        #region IStorageCrud Implementation

        public string TableName
        {
            get
            {
                return _dynamicStorage.TableName;
            }

            set
            {
                _dynamicStorage.TableName = value;
            }
        }

        public string PartitionName
        {
            get
            {
                return _dynamicStorage.PartitionName;
            }

            set
            {
                _dynamicStorage.PartitionName = value;
            }
        }

        public string StorageConnectionString
        {
            get
            {
                return _dynamicStorage.StorageConnectionString;
            }

            set
            {
                _dynamicStorage.StorageConnectionString = value;
            }
        }

        public async Task<JObject> CreateAsync(JObject model)
        {
            // Map into a DynamicTableEntity
            DynamicTableEntity entity = model.ToDynamicEntity();
            entity.RowKey = KeyGenerator.NextTicksDescendingRowKey();
            // Create and return as JObject
            entity = await _dynamicStorage.CreateAsync(entity);
            return entity.ToJObject();
        }

        public async Task<IEnumerable<JObject>> ReadAllAsync()
        {
            IEnumerable<DynamicTableEntity> tableEntities = await _dynamicStorage.ReadAllAsync();
            return tableEntities.Select(te => te.ToJObject());
        }

        public async Task<JObject> ReadAsync(string id)
        {
            DynamicTableEntity tableEntity = await _dynamicStorage.ReadAsync(id);

            if (tableEntity == null)
                throw new Exception(string.Format("List item {0} cannot be found", id));

            return tableEntity.ToJObject();
        }

        public async Task<JObject> UpdateAsync(JObject model)
        {
            DynamicTableEntity entity = model.ToDynamicEntity();
            entity = await _dynamicStorage.UpdateAsync(entity);
            return entity.ToJObject();
        }

        public async Task DeleteAsync(string id)
        {
            await _dynamicStorage.DeleteAsync(id);
        }

        #endregion
    }
}
