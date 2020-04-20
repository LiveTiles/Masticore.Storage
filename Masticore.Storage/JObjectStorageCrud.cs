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

        public async Task<JObject> CreateAsync(JObject model)
        {
            // Map into a DynamicTableEntity
            var entity = model.ToDynamicEntity();
            entity.RowKey = KeyGenerator.NextTicksDescendingRowKey();
            // Create and return as JObject
            entity = await _dynamicStorage.CreateAsync(entity);
            return entity.ToJObject();
        }

        public async Task<IEnumerable<JObject>> ReadAllAsync()
        {
            var tableEntities = await _dynamicStorage.ReadAllAsync();
            return tableEntities.Select(te => te.ToJObject());
        }

        public async Task<JObject> ReadAsync(string id)
        {
            var tableEntity = await _dynamicStorage.ReadAsync(id);

            if (tableEntity == null)
                throw new Exception(string.Format("List item {0} cannot be found", id));

            return tableEntity.ToJObject();
        }

        public async Task<JObject> UpdateAsync(JObject model)
        {
            var entity = model.ToDynamicEntity();
            entity = await _dynamicStorage.UpdateAsync(entity);
            return entity.ToJObject();
        }

        public async Task DeleteAsync(string id)
        {
            await _dynamicStorage.DeleteAsync(id);
        }

        public Func<string> GetStorageConnectionString
        {
            get => _dynamicStorage.GetStorageConnectionString;
            set => _dynamicStorage.GetStorageConnectionString = value;
        }
    }
}
