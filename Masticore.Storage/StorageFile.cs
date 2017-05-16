using System.ComponentModel.DataAnnotations;

namespace Masticore.Storage
{
    /// <summary>
    /// Standard implementation of IFile for Azure Storage
    /// </summary>
    public class StorageFile : PersistentBase<int>, IFile, INamed
    {
        /// <summary>
        /// Gets or sets the URL for the Cloud-hosted file
        /// </summary>
        public string Url { get; set; }

        /// <summary>
        /// Gets or sets the display name of the cloud file
        /// </summary>
        [MaxLength(256)]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the container name of the cloud file
        /// </summary>
        [Required]
        [MinLength(3)]
        [MaxLength(63)]
        [Merge(AllowUpdate = false)]
        public string ContainerName { get; set; }

        /// <summary>
        /// Gets or sets the file name
        /// </summary>
        [Required]
        [MinLength(1)]
        [MaxLength(1024)]
        [Merge]
        public string FileName { get; set; }
    }
}
