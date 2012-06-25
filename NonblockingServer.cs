using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Collections;
using System.Threading;

class StateObject {
    public byte[] buffer = new byte[1024*1024 + 250];
    public int totalReceived = 0;
    public Socket socket;
}

public class NonblockingServer {
    public static int totalToRecv = 0;
    public static ManualResetEvent clientConnected = new ManualResetEvent(false);
    public static Socket server;

    public static void doBeginAcceptSocket() {
        Console.WriteLine("Wait for connection...");

        server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        IPEndPoint ipLocal = new IPEndPoint(IPAddress.Any, 1234);
        server.Bind(ipLocal);
        server.Listen(10);
        server.BeginAccept(new AsyncCallback(DoAcceptSocketCallback), server);
    }

    public static void DoAcceptSocketCallback(IAsyncResult ar) {
        clientConnected.Reset();
        Socket listener = (Socket) ar.AsyncState;

        Socket socket = listener.EndAccept(ar);
//        clientConnected.Set();

        Console.WriteLine("Socket accepted!");
        Console.WriteLine("Begin reading...");

        StateObject so = new StateObject();
        so.socket = socket;
        
        socket.BeginReceive(so.buffer, 0, 64*1024, 0, new AsyncCallback(ReadCallback), so);
        server.BeginAccept(new AsyncCallback(DoAcceptSocketCallback), server);
    }

    public static void ReadCallback(IAsyncResult ar) {
        StateObject so = (StateObject) ar.AsyncState;
        Socket socket = so.socket;

        int byteRead = socket.EndReceive(ar);

        if (byteRead > 0) {
            so.totalReceived += byteRead;
            Console.WriteLine("{0} received!", so.totalReceived);
            
            if (so.totalReceived < totalToRecv) {
                socket.BeginReceive(so.buffer, 0, 64*1024, 0, new AsyncCallback(ReadCallback), so);
            } else {
                Console.WriteLine("Connection closed!");
                socket.Close();
            }
        } else {
            Console.WriteLine("Connection closed!");
            socket.Close();
        }
    }

    public static void Main(String[] args) {
        totalToRecv = Int32.Parse(args[0]);
        doBeginAcceptSocket();
        clientConnected.WaitOne();
    }
}
