using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;

var cancellationTokenSource = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cancellationTokenSource.Cancel(); Console.WriteLine("Cancellation requested, shutting down... Wait to finish all tasks."); };

// Producer -> CPU workers.
var inputQueue = new BlockingCollection<SensorReading>(boundedCapacity: 32768);

// CPU workers -> Consumer.
var outputQueue = new BlockingCollection<string>(boundedCapacity: 32768);

// Start dedicated producer thread, which simulates I/O-bound work by generating sensor readings.
// If the producer listens any network or disk I/O, it should be run on main thread, so can run as Task,
// but now we need a dedicated thread for CPU-bound work.
var producerThread = new Thread(() => ProduceLoop(inputQueue, cancellationTokenSource.Token))
{
    IsBackground = true,
    Name = "producer"
};
producerThread.Start();

var cpuThreadCount = Math.Max(1, Environment.ProcessorCount - 1);
var cpuWorkerThreads = new Thread[cpuThreadCount];
for (var i = 0; i < cpuWorkerThreads.Length; i++)
{
    cpuWorkerThreads[i] = new Thread(() => CpuWorkerLoop(inputQueue, outputQueue))
    {
        IsBackground = true,
        Name = $"cpu-worker-{i}"
    };
    cpuWorkerThreads[i].Start();
}

// No need to dedicate a thread for the consumer, as it can run on the main thread.
// While waiting(await Task.WhenAll) for I/O, the main thread can be used for other tasks.
var consumerTask = Task.Run(() => ConsumerLoop(outputQueue));

try
{
    await Task.Delay(TimeSpan.FromMinutes(1), cancellationTokenSource.Token);
}
catch (OperationCanceledException)
{
}
catch (ObjectDisposedException)
{
}
catch (Exception ex)
{
    Console.WriteLine($"An error occurred: {ex.Message}");
}
finally
{
    cancellationTokenSource.Cancel();

    producerThread.Join();
    inputQueue.CompleteAdding();

    foreach (var t in cpuWorkerThreads)
        t.Join();

    outputQueue.CompleteAdding();
    await consumerTask;

    Console.WriteLine($"InputQueue:  Count={inputQueue.Count},  IsCompleted={inputQueue.IsCompleted}");
    Console.WriteLine($"OutputQueue: Count={outputQueue.Count}, IsCompleted={outputQueue.IsCompleted}");
}



Console.WriteLine("We are okay, all done!");
return 0;

static void ProduceLoop(BlockingCollection<SensorReading> input, CancellationToken cancellationToken)
{
    Console.WriteLine($"Starting producer on thread {Environment.CurrentManagedThreadId}.");

    var rnd = new Random();
    var id = 0;
    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            var values = new double[128];
            for (var i = 0; i < values.Length; i++)
                values[i] = rnd.NextDouble() * 2000 - 1000;

            var reading = new SensorReading(Id: ++id, Values: values, CapturedAt: DateTime.UtcNow);

            // Add to the input queue
            if (!input.IsAddingCompleted)
                input.Add(reading, cancellationToken);

            // Simulate some delay
            cancellationToken.WaitHandle.WaitOne(5);
        }
        catch (OperationCanceledException)
        {
            break;
        }
        catch (ObjectDisposedException)
        {
            break;
        }
    }
}

// CPU-BOUND WORKER
// Reads from the input queue, processes the data, and writes to the output queue.
static void CpuWorkerLoop(BlockingCollection<SensorReading> input, BlockingCollection<string> output)
{
    Console.WriteLine($"Starting CPU worker on thread {Environment.CurrentManagedThreadId} for CPU-BOUND processing");

    var jsonOptions = new JsonSerializerOptions { WriteIndented = false };

    try
    {
        // Cancellation token not used here, as we want to process all items in the input queue.
        foreach (var item in input.GetConsumingEnumerable(CancellationToken.None))
        {
            // CPU-BOUND simplification + conversion to JSON
            var simplified = Simplify(item);
            var json = JsonSerializer.Serialize(simplified, jsonOptions);

            if (!output.IsAddingCompleted)
                output.Add(json, CancellationToken.None);
        }
    }
    catch (OperationCanceledException)
    {
    }
    catch (ObjectDisposedException)
    {
    }

    return;

    static SensorReading Simplify(SensorReading input)
    {
        var trimmed = new double[input.Values.Length];
        for (var i = 0; i < trimmed.Length; i++)
        {
            var v = input.Values[i];

            if (v < -1e6)
                v = -1e6;

            if (v > 1e6)
                v = 1e6;

            trimmed[i] = v / 1000.0;
        }

        return input with { Values = trimmed };
    }
}

static async Task ConsumerLoop(BlockingCollection<string> output)
{
    Console.WriteLine("Starting sensor data consumer");
    var dir = Path.Combine(AppContext.BaseDirectory, "out");
    Directory.CreateDirectory(dir);

    const string endpoint = "https://example.org/api/readings";
    using var httpClient = new HttpClient();
    httpClient.Timeout = TimeSpan.FromSeconds(10);

    try
    {
        // Cancellation token not used here, as we want to process all items in the output queue.
        foreach (var json in output.GetConsumingEnumerable(CancellationToken.None))
        {
            var fileName = Path.Combine(dir, $"{DateTime.UtcNow:yyyyMMdd_HHmmss_ffff}.json");

            // Start two I/O-bound tasks
            var write = WriteJsonToDiskAsync(fileName, json, CancellationToken.None);
            var post = PostJsonAsync(httpClient, endpoint, json, CancellationToken.None);

            // Wait for both tasks to complete
            await Task.WhenAll(write, post);
        }
    }
    catch (OperationCanceledException)
    {
    }
    catch (ObjectDisposedException)
    {
    }
}

static async Task WriteJsonToDiskAsync(string path, string json, CancellationToken ct)
{
    await using var fileStream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 8192, useAsync: true);
    await using var writer = new StreamWriter(fileStream, new UTF8Encoding(false));
    await writer.WriteAsync(json.AsMemory(), ct);
}

static async Task PostJsonAsync(HttpClient http, string url, string json, CancellationToken ct)
{
    using var content = new StringContent(json, Encoding.UTF8, "application/json");
    await Task.Delay(10, ct); // Simulate network delay
    //var resp = await http.PostAsync(url, content, ct);
    //resp.EnsureSuccessStatusCode();
}

record SensorReading(int Id, double[] Values, DateTime CapturedAt);