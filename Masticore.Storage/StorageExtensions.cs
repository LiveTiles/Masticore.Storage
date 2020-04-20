using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using System;
using System.Threading.Tasks;

namespace Masticore.Storage
{
	/// <summary>
	/// Miscellaneous extensions for classes in the Microsoft.Windows.Azure.Storage and related namespaces
	/// </summary>
	public static class StorageExtensions
	{
		/// <summary>
		/// Allows caller to replace or alter the current CorsProperties on a given CloudStorageAccount.
		/// </summary>
		/// <param name="storageAccount">Storage account.</param>
		/// <param name="alterCorsRules">The returned value will replace the 
		/// current ServiceProperties.Cors (ServiceProperties) value. </param>
		public static void SetCorsProperties(this CloudStorageAccount storageAccount,
			Func<CorsProperties, CorsProperties> alterCorsRules)
		{
			if (storageAccount == null || alterCorsRules == null) throw new ArgumentNullException();

			var blobClient = storageAccount.CreateCloudBlobClient();

			var serviceProperties = blobClient.GetServiceProperties();

			serviceProperties.Cors = alterCorsRules(serviceProperties.Cors) ?? new CorsProperties();

			blobClient.SetServiceProperties(serviceProperties);
		}
		/// <summary>
		/// Sets the REST API version on the Blob Services.
		/// </summary>
		/// <param name="storageAccount">Storage account.</param>
		/// <param name="defaultServiceVersion">The returned value will replace the current 
		/// ServiceProperties.DefaultServiceVersion </param>
		public static void SetServiceVersion(this CloudStorageAccount storageAccount, string defaultServiceVersion)
		{
			if (storageAccount == null || string.IsNullOrEmpty(defaultServiceVersion))
				throw new ArgumentNullException();

			var blobClient = storageAccount.CreateCloudBlobClient();
			var serviceProperties = blobClient.GetServiceProperties();
			serviceProperties.DefaultServiceVersion = defaultServiceVersion;

			blobClient.SetServiceProperties(serviceProperties);


		}
		/// <summary>
		/// Enables CORS for this account for all (*) endpoings
		/// </summary>
		/// <param name="account"></param>
		public static void SetCorsForAll(this CloudStorageAccount account)
		{
			account.SetCorsProperties(cors =>
			{
				var wildcardRule = new CorsRule() { AllowedMethods = CorsHttpMethods.Get | CorsHttpMethods.Head, AllowedOrigins = { "*" } };
				cors.CorsRules.Clear();
				cors.CorsRules.Add(wildcardRule);
				return cors;
			});
		}
		/// <summary>
		/// Set the properties (e.g., service version, logging, CORS) on the blob service associated to the storage account.  
		/// </summary>
		/// <param name="account">The Cloud Storage Account</param>
		/// <param name="serviceProperties">The service properties to create/update</param>
		/// <returns></returns>
		public static async Task SetServiceProperties(this CloudStorageAccount account, ServiceProperties serviceProperties)
		{
			if (account == null || serviceProperties == null)
				throw new ArgumentNullException();

			var blobClient = account.CreateCloudBlobClient();
			await blobClient.SetServicePropertiesAsync(serviceProperties);

		}
	}
}
