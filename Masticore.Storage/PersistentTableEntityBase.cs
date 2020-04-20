
using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace Masticore.Storage
{
    /// <summary>
    /// A general purpose base entity for a persistent table
    /// </summary>
    public abstract class PersistentTableEntityBase : TableEntity, IPersistent<string>
    {
        [IgnoreProperty]
        public string Id
        {
            get { return RowKey; }

            set { value = RowKey; }
        }

        [Merge(AllowCreate = false)]
        public DateTime? UpdatedUtc { get; set; }

        [Merge(AllowUpdate = false)]
        public DateTime? CreatedUtc { get; set; }

        public DateTime? DeletedUtc { get; set; }
    }

    /// <summary>
    ///  IPersistent of string + IUniversal over TableEntity
    /// </summary>
    public abstract class UniversalPersistentTableEntityBase : PersistentTableEntityBase, IUniversal
    {
        [Merge(AllowOnce = true)]
        public string UniversalId { get; set; }
    }
}
