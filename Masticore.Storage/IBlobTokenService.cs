namespace Masticore.Storage
{
    public interface IBlobTokenService
    {
        string GetFileUrlWithToken(string url);
    }
}