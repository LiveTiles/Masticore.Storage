namespace Masticore.Storage
{
    public class BlobTokenService : BlobStorageBase, IBlobTokenService
    {
        public string GetFileUrlWithToken(string url)
        {
            return AddSasToken(url);
        }
    }
}
