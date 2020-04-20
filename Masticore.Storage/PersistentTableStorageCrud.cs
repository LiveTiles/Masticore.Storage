using Microsoft.WindowsAzure.Storage.Table;

namespace Masticore.Storage
{
    /// <summary>
    /// Implementation of TableEntityStorageCrud for Persisted Tables
    /// </summary>
    public abstract class PersistentTableStorageCrud<TableEntityType> : TableEntityStorageCrud<TableEntityType>
        where TableEntityType : class, IPersistent<string>, IUniversal, ITableEntity, new()
    {
        public PersistentTableStorageCrud(IStorageTableFactory tableFactory) : base(tableFactory)
        {
        }
    }
}
