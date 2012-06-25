using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using System.Collections.Generic;
using System.Collections;
using System.Threading;
using Isis;

namespace IsisService {
	class StateObject {
		public int bufferSize = 1024 * 1024 + 250;
		public byte[] recvBuffer = new byte[1024 * 1024 + 250];
		public int totalReceived = 0;
		public byte[] writeBuffer;
		public int totalWrote = 0;
		public Socket socket;

	}

	public class NonblockingServer {
		public static ManualResetEvent clientConnected = new ManualResetEvent(false);
		public static Socket server;
		public static Isis.Group[] shardGroup;
		public static Isis.Timeout timeout = new Isis.Timeout(IsisServer.timeout, Isis.Timeout.TO_FAILURE);
		
		//Accept callback
		public static void doBeginAcceptSocket() {
		    Console.WriteLine("Waiting for a connection...");

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

		    if (Parameter.isVerbose) {
		    	Console.WriteLine("Client accepted!");
		    }

		    StateObject so = new StateObject();
		    so.socket = socket;
		    
		    socket.BeginReceive(so.recvBuffer, so.totalReceived, so.bufferSize - so.totalReceived, 0, new AsyncCallback(readCallback), so);
		    server.BeginAccept(new AsyncCallback(DoAcceptSocketCallback), server);
		}

		//Read callback
		public static void readCallback(IAsyncResult ar) {
		    StateObject so = (StateObject) ar.AsyncState;
		    Socket socket = so.socket;

		    int byteRead = socket.EndReceive(ar);

		    if (byteRead > 0) {
		        so.totalReceived += byteRead;
		        Console.WriteLine("{0} received!", so.totalReceived);
		        
		        string str = Encoding.ASCII.GetString(so.recvBuffer, 0, so.totalReceived);
		        Console.Write(str);
		        
		        string[] words = Regex.Split(str, "\r\n");
		       	if (words[words.Length - 2] == "") {
		       		Console.WriteLine("End of input!");
		       		int commandType = 0;
		       		
		       		if (words[0] == "insert") {
		       			commandType = IsisServer.INSERT;
		       		}
		       		
		       		if (words[0] == "get") {
		       			commandType = IsisServer.GET;
		       		}
		       		
		       		List<string> replyList = new List<string>();
		       		string toSend = "";
		       		if (commandType == IsisServer.INSERT) {
		       			toSend = Encoding.ASCII.GetString(so.recvBuffer, 8, so.totalReceived - 2);
		       		} else {
		       			toSend = Encoding.ASCII.GetString(so.recvBuffer, 5, so.totalReceived - 2);
		       		}
		       		
		       		int nr = shardGroup[0].Query(Isis.Group.ALL, timeout, commandType, toSend, shardGroup[0].GetView().GetMyRank(), new EOLMarker(), replyList);
		       		
		       		if (Parameter.isVerbose) {
		       			foreach (string s in replyList) {
		       				Console.WriteLine("Received reply {0}", s);
		       			}
		       		}
		       		
		       		string reply;
		       		string setCmd;
		       		
		       		switch (commandType) {
		       			case 0:
		       				reply = "OK.\n";
		       				so.writeBuffer = Encoding.ASCII.GetBytes(reply);
		       				socket.BeginSend(so.writeBuffer, 0, so.writeBuffer.Length, 0, new AsyncCallback(writeCallback), so);
		       				break;
		       			
		       			case 1:
		       				reply = "END\r\n";
		       				foreach (string s in replyList) {
		       					if (s != "END\r\n") {
		       						reply = s;
		       						break;
		       					}
		       				}
		       				so.writeBuffer = Encoding.ASCII.GetBytes(reply);
		       				socket.BeginSend(so.writeBuffer, 0, so.writeBuffer.Length, 0, new AsyncCallback(writeCallback), so);
		       				
		       				if (reply != "END\r\n") {
								words = Regex.Split(reply, "\r\n");
								string[] word = Regex.Split(words[0], " ");
								setCmd = "set " + word[1] + " 4 0 " + word[3] + "\r\n";
								setCmd += words[1];
								setCmd += "\r\n";
										
								reply = Parameter.talkToMem(setCmd, 0);
								if (Parameter.isVerbose) {
									Console.WriteLine(reply);
								}
							}
							break;
		       		}
		       		so.totalReceived = 0;
		       		socket.BeginReceive(so.recvBuffer, so.totalReceived, so.bufferSize - so.totalReceived, 0, new AsyncCallback(readCallback), so);
		       	} else {
		        	socket.BeginReceive(so.recvBuffer, so.totalReceived, so.bufferSize - so.totalReceived, 0, new AsyncCallback(readCallback), so);
		        }
		    } else {
		        Console.WriteLine("A client left!");
		        socket.Close();
		    }
		}
		
		//Write call back
		public static void writeCallback(IAsyncResult ar) {
			StateObject so = (StateObject) ar.AsyncState;
		    Socket socket = so.socket;
		    
		    int bytesWrote = socket.EndSend(ar);
		    so.totalWrote += bytesWrote;
		    
		    //All wrote
		    if (so.totalWrote == so.writeBuffer.Length) {
		    	so.totalWrote = 0;
		    	so.writeBuffer = null;
		    } else {
		    	socket.BeginSend(so.writeBuffer, so.totalWrote, so.writeBuffer.Length - so.totalWrote, 0, new AsyncCallback(writeCallback), so);
		    }
		}
	}
}
