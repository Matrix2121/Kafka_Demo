using DotNetEnv;

class Program
{
    private static void Main(string[] args)
    {
        Env.Load();

        MultiPurposeTrackingProducer.Start().GetAwaiter().GetResult();
        //MultiPurposeNonTrackingProducer.Start().GetAwaiter().GetResult();
    }
}

