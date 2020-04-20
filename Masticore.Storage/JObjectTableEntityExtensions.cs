using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;

namespace Masticore.Storage
{
    /// <summary>
    /// Extensions methods for converting JObject to DynamicTableEntity and back
    /// </summary>
    public static class JObjectTableEntityExtensions
    {
        /// <summary>
        /// Constant for the name of the RowKey field when applied to a JObject to/from a DynamicTableEntity
        /// </summary>
        public const string JObjectRowKeyFieldName = "Id";
        /// <summary>
        /// Constant for the name of ETag 
        /// </summary>
        public const string ETag = "ETag";
        /// <summary>
        /// Converts the given JObject to a new DynamicTableEntity instance
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static DynamicTableEntity ToDynamicEntity(this JObject model)
        {
            string rowKey = null;
            string eTag = null;

            if (model[JObjectRowKeyFieldName] != null)
            {
                rowKey = model[JObjectRowKeyFieldName].Value<string>();
                model.Remove(JObjectRowKeyFieldName);
            }
            if (model[ETag] != null)
            {
                eTag = model[ETag].Value<string>();
                model.Remove(ETag);
            }
            // Map into a DynamicTableEntity
            var entity = new DynamicTableEntity();
            foreach (var prop in model.Properties())
            {
                if (prop.Value != null)
                {
                    EntityProperty entityProp = prop.Value.ToEntityProperty();
                    if (entityProp != null)
                        entity.Properties.Add(prop.Name, entityProp);
                }
            }

            entity.RowKey = rowKey;
            entity.ETag = eTag;

            return entity;
        }

        /// <summary>
        /// Maps an individual JToken into a new EntityProperty value
        /// This currently support boolean, string, integer, float, and date - all other formats return null
        /// </summary>
        /// <param name="prop"></param>
        /// <returns></returns>
        public static EntityProperty ToEntityProperty(this JToken prop)
        {
            switch (prop.Type)
            {
                case JTokenType.Boolean:
                    return new EntityProperty(prop.Value<bool>());
                case JTokenType.String:
                    return new EntityProperty(prop.Value<string>());
                case JTokenType.Integer:
                    return new EntityProperty(prop.Value<int>());
                case JTokenType.Float:
                    return new EntityProperty(prop.Value<float>());
                case JTokenType.Date:
                    return new EntityProperty(DateTime.SpecifyKind(DateTime.Parse(prop.ToString()), DateTimeKind.Utc));
                default:
                    return null;
            }
        }

        /// <summary>
        /// Converts a DynamicTableEntity to a JObject
        /// This will automatically include the ETag, Timestamp, and RowKey field (as ID)
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static JObject ToJObject(this DynamicTableEntity entity)
        {
            var entityList = entity.Properties.Select(x => new JProperty(x.Key, x.Value.PropertyAsObject)).ToArray();
            var obj = new JObject(entityList);

            //Add in our tracking properties
            if (obj.Value<string>(nameof(entity.ETag)) == null)
                obj.AddFirst(new JProperty(nameof(entity.ETag), entity.ETag));
            if (obj.Value<string>(nameof(entity.Timestamp)) == null)
                obj.AddFirst(new JProperty(nameof(entity.Timestamp), entity.Timestamp.UtcDateTime));
            if (obj.Value<string>(nameof(entity.RowKey)) == null)
                obj.AddFirst(new JProperty(JObjectRowKeyFieldName, entity.RowKey));

            return obj;
        }
    }
}
