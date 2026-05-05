using System;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;

class Program
{
    static async Task Main(string[] args)
    {
        var client = new ChatClient("ws://localhost:3000/ws");
        await client.Start();
    }
}

class ChatClient
{
    private ClientWebSocket _ws = new();
    private readonly Uri _uri;

    private string _username = "";
    private string _currentInput = "";

    private ConcurrentQueue<string> _messages = new();

    public ChatClient(string url)
    {
        _uri = new Uri(url);
    }

    public async Task Start()
    {
        await _ws.ConnectAsync(_uri, CancellationToken.None);

        Console.Write("Username: ");
        _username = Console.ReadLine() ?? "Anonymous";

        await Send(new { action = "join", rooms = new[] { "world", "guild", "party" } });

        _ = Task.Run(ReceiveLoop);
        _ = Task.Run(InputLoop);

        RenderLoop();
    }

    async Task ReceiveLoop()
    {
        var buffer = new byte[4096];

        while (_ws.State == WebSocketState.Open)
        {
            var result = await _ws.ReceiveAsync(
                new ArraySegment<byte>(buffer),
                CancellationToken.None
            );

            var msg = Encoding.UTF8.GetString(buffer, 0, result.Count);

            try
            {
                var data = JsonSerializer.Deserialize<Message>(msg);
                if (data != null)
                {
                    var line = $"[{data.room}] [{data.username}] {data.text}";
                    _messages.Enqueue(line);
                }
            }
            catch { }
        }
    }

    async Task InputLoop()
    {
        while (true)
        {
            var key = Console.ReadKey(true);

            if (key.Key == ConsoleKey.Enter)
            {
                var text = _currentInput.Trim();
                _currentInput = "";

                if (!string.IsNullOrEmpty(text))
                {
                    await Send(new
                    {
                        action = "send",
                        rooms = new[] { "world" },
                        text = text,
                        username = _username
                    });
                }
            }
            else if (key.Key == ConsoleKey.Backspace)
            {
                if (_currentInput.Length > 0)
                    _currentInput = _currentInput[..^1];
            }
            else
            {
                _currentInput += key.KeyChar;
            }
        }
    }

    void RenderLoop()
    {
        while (true)
        {
            Console.Clear();

            foreach (var msg in _messages)
            {
                Console.WriteLine(msg);
            }

            Console.Write("> " + _currentInput);

            Thread.Sleep(50); // smooth refresh
        }
    }

    async Task Send(object obj)
    {
        var json = JsonSerializer.Serialize(obj);
        var bytes = Encoding.UTF8.GetBytes(json);

        await _ws.SendAsync(
            new ArraySegment<byte>(bytes),
            WebSocketMessageType.Text,
            true,
            CancellationToken.None
        );
    }
}

class Message
{
    public string room { get; set; } = "";
    public string text { get; set; } = "";
    public string username { get; set; } = "";
}