using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using System.Collections.Generic;
using System.Collections;
using System.Threading;
using Isis;

namespace IsisService {
	//Blocking socket server
	public  class SynchronousSocketListener {
		private static ArrayList ClientSockets;       //Array to store client sockets
		private static bool ContinueReclaim = true;   //If continue reclaim
	 	private static Thread ThreadReclaim;          //Reclaim thread
		public static Isis.Group[] shardGroup;        //Isis group
		
		//Start listening
	  	public  static  void StartListening() {
			ClientSockets = new ArrayList() ;
			int ClientNbr = 0;
			ThreadReclaim = new Thread(new ThreadStart(Reclaim));
			ThreadReclaim.Start() ;
		
			TcpListener listener = new TcpListener(Parameter.portNum);
			try {
		    	listener.Start();
		    
		        // Start listening for connections.
		        if (Parameter.isVerbose) {
		        	Console.WriteLine("Waiting for a connection...");
		        }
		        while (true) {
		        	TcpClient handler = listener.AcceptTcpClient();
		                    
		            if (handler != null)  {
		            	if (Parameter.isVerbose) {
	 	            		Console.WriteLine("Client#{0} accepted!", ++ClientNbr);
		           	    }
		           	    // An incoming connection needs to be processed.
		            	lock( ClientSockets.SyncRoot ) {
		                	int i = ClientSockets.Add(new ClientHandler(handler, shardGroup));
		                    ((ClientHandler) ClientSockets[i]).Start();
		                }
		            }            
		        }
		        listener.Stop();
		          
		        ContinueReclaim = false ;
		        ThreadReclaim.Join();
		          
		        foreach ( Object Client in ClientSockets ) {
		        	( (ClientHandler) Client ).Stop();
		        }
		        
			} catch (Exception e) {
		    	Console.WriteLine(e.ToString());
			}
		    
			Console.WriteLine("\nHit enter to continue...");
			Console.Read();
		}
	
	  	private static void Reclaim()  {
		    while (ContinueReclaim) {
		    	lock(ClientSockets.SyncRoot) {
		        	for (int x = ClientSockets.Count-1; x >= 0; x-- )  {
		                    Object Client = ClientSockets[x];
		                    if (!( ( ClientHandler ) Client ).Alive )  {
		                    	ClientSockets.Remove( Client );
		                    	if (Parameter.isVerbose) {
		                        	Console.WriteLine("A client left");
		                        }
		                    }
		            }
		        }
		        Thread.Sleep(200);
		    }         
	  	}
	}

	//Client handler class
	class ClientHandler {

		TcpClient ClientSocket ;
		bool ContinueProcess = false;
		Thread ClientThread;
		Isis.Group[] myGroup;
		Isis.Timeout timeout;
		const int INSERT = 0;
		const int GET = 1;
		
		public ClientHandler (TcpClient ClientSocket, Isis.Group[] myGroup) {
			this.ClientSocket = ClientSocket;
			this.myGroup = myGroup;
			this.timeout = new Isis.Timeout(IsisServer.timeout, Isis.Timeout.TO_FAILURE);
		}

		public void Start() {
			ContinueProcess = true ;
			ClientThread = new Thread (new ThreadStart(Process));
			ClientThread.Start();
		}

		private  void Process() {
			// Data buffer for incoming data.
			byte[] bytes;
		    int readBytes = 0;
		    string line;
		    
			if (ClientSocket != null) {
		    	NetworkStream networkStream = ClientSocket.GetStream();

				using (StreamReader reader = new StreamReader(ClientSocket.GetStream(), System.Text.Encoding.ASCII)) {
					string command = "";
					int commandType = 0;
					
					while ((line = reader.ReadLine()) != null) {
						if (Parameter.isVerbose) {
							Console.WriteLine("Received a line {0} from client", line);
						}
						
						if (line == "insert") {
							commandType = IsisServer.INSERT;
							continue;
						}
						
						if (line == "get") {
							commandType = IsisServer.GET;
							continue;
						}
						
						//End of command, use ISIS to send the command!
						if (line == "") {
							List<string> replyList = new List<string>();
							
							int	nr = myGroup[0].Query(Isis.Group.ALL, timeout, commandType, command, myGroup[0].GetView().GetMyRank(), new EOLMarker(), replyList);

							if (Parameter.isVerbose) {
								foreach (string s in replyList) {
									Console.WriteLine("Received reply {0}", s);
								}
							}
							
							byte[] sendBytes;
							string reply;
							string setCmd;
							
							//Send reply to memcached
							switch (commandType) {
								//Insert reply
								case INSERT:
									reply = "OK.\n";
									sendBytes = Encoding.ASCII.GetBytes(reply);
									networkStream.Write(sendBytes, 0, sendBytes.Length);
									break;
								
								//Get reply
								case GET:
									reply = "END\r\n";
									foreach (string s in replyList) {
										if (s != "END\r\n") {
											reply = s;
											break;
										}
									}
									sendBytes = Encoding.ASCII.GetBytes(reply);
									networkStream.Write(sendBytes, 0, sendBytes.Length);
									
									//If there is a value, ask current memcached to store
									if (reply != "END\r\n") {
										string[] words = Regex.Split(reply, "\r\n");
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
							
							command = "";
						} else {
							command += line;
							command += "\r\n";
						}
					}
				}
			   	 
		        networkStream.Close();
		    	ClientSocket.Close();			
		    	
		    	if (Parameter.isVerbose) {
		        	Console.WriteLine("Connection closed!");
		        }
			}
		}  // Process()

		public void Stop() 	{
			ContinueProcess = false;
		    if (ClientThread != null && ClientThread.IsAlive) {
				ClientThread.Join();
			}
		}
		    
		public bool Alive {
			get {
				return  (ClientThread != null  && ClientThread.IsAlive);
			}
	   	}      
	} 
}
