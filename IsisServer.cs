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

//Works but quite lousy design!!!
namespace IsisService {
	delegate void insert(string command, int rank);
	delegate void query(string command, int rank);
	
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

	//Main class
	class MainClass {
		//Main
	  	public static int Main(String[] args) {
			int i = 0;
			int nodeNum = -1;
			int myRank = -1;
			int shardSize = -1;
			
			while (i < args.Length) {
				//port number
				if (args[i] == "-p") {
					Parameter.portNum = Int32.Parse(args[++i]);
					i++;
				} else if (args[i] == "-v") {
					Parameter.isVerbose = true;
					i++;
				} else if (args[i] == "-n") {
					nodeNum = Int32.Parse(args[++i]);
					i++;
				} else if (args[i] == "-r") {
					myRank = Int32.Parse(args[++i]);
					i++;
				} else if (args[i] == "-s") {
					shardSize = Int32.Parse(args[++i]);
					i++;
				} else if (args[i] == "-t") {
					IsisServer.timeout = Int32.Parse(args[++i]);
					i++;
				} else if (args[i] == "-m") {
					Parameter.memPortNum = Int32.Parse(args[++i]);
					i++;
				} else {
					Console.WriteLine("Unknown argument!");
					Parameter.printUsage();
					return 0;
				}
			}
			
			if (nodeNum == -1 || myRank == -1 || shardSize == -1) {
				Console.WriteLine("Total node number, my rank and shard size have to be specified!");
				Parameter.printUsage();
				return 0;
			}
			
			if (myRank >= nodeNum) {
				Console.WriteLine("Your rank can't be equal to or larger than total node number!");
				return 0;
			}
			
			if (shardSize > nodeNum) {
				Console.WriteLine("Shard size can't be larger than node number!");
				return 0;
			}
			
			if (Parameter.isVerbose) {
				Console.WriteLine("Listening port number is {0}", Parameter.portNum);
				Console.WriteLine("Total node number is {0}", nodeNum);
				Console.WriteLine("My rank is {0}", myRank);
				Console.WriteLine("Shard size is {0}", shardSize);
			}
			
			IsisServer isisServer = new IsisServer(nodeNum, shardSize, myRank);
			isisServer.createGroup();
			while (isisServer.allJoin == false);
			SynchronousSocketListener.shardGroup = isisServer.shardGroup;
			SynchronousSocketListener.StartListening();
			return 0;
	  	}
	}

	class IsisServer {
		public int nodeNum;
		public int shardSize;
		public int myRank;
		public bool[] groupJoin;
		public bool allJoin = false;
		public Isis.Group[] shardGroup;       //Shard group
		public static int timeout = 15000;    //Timeout. Default: 15 sec
		public static int INSERT = 0;   //Insert number
		public static int GET = 1;      //Get number

		
		public IsisServer(int nodeNum, int shardSize, int myRank) {
			this.nodeNum = nodeNum;
			this.shardSize = shardSize;
			this.myRank = myRank;
		}

	  	public void createGroup() {
	  		IsisSystem.Start();
	  		if (Parameter.isVerbose) {
	  			Console.WriteLine("Isis system started!");
	  		}
	  		
	  		shardGroup = new Isis.Group[shardSize];
	  		groupJoin = new bool[shardSize];
	  		
	  		int groupNum = myRank;
	  		for (int i = 0; i < shardSize; i++) {
	  			shardGroup[i] = new Isis.Group("group"+groupNum);
	  			groupJoin[i] = false;
	  			
	  			groupNum--;
	  			if (groupNum < 0) {
	  				groupNum += nodeNum;
	  			}
	  		}
	  		
	  		for (int i = 0; i < shardSize; i++) {
	  			int local = i;
	  			
	  			//Insert handler
	  			shardGroup[i].Handlers[INSERT] += (insert)delegate(string command, int rank) {
	  				if (Parameter.isVerbose) {
	  					Console.WriteLine("Got a command {0}", command);
	  				}
	  				
	  				if (shardGroup[local].GetView().GetMyRank() == rank) {
	  					if (Parameter.isVerbose) {
	  						Console.WriteLine("Got a message from myself!");
	  					}
	  					shardGroup[local].Reply("Yes");
	  				} else {
	  					string ret = Parameter.talkToMem(command, INSERT);
	  					if (ret == "STORED") {
	  						shardGroup[local].Reply("Yes");
	  					} else {
	  						shardGroup[local].Reply("No");
	  					}
	  				}
	  			};
	  			
	  			//Get handler
	  			shardGroup[i].Handlers[GET] += (query)delegate(string command, int rank) {
	  				if (Parameter.isVerbose) {
	  					Console.WriteLine("Got a command {0}", command);
	  				}
  					if (shardGroup[local].GetView().GetMyRank() == rank) {
  						if (Parameter.isVerbose) {
  							Console.WriteLine("Got a message from myself!");
  						}
  						shardGroup[local].Reply("END\r\n"); //Definitely not presented in local memcached!
  					} else {
  						string ret = Parameter.talkToMem(command, GET);
  						shardGroup[local].Reply(ret);
  					}
	  			};
	  			
	  			//View handler
	  			shardGroup[i].ViewHandlers += (Isis.ViewHandler)delegate(View v) {
	  				if (Parameter.isVerbose) {
	  					Console.WriteLine("Got a new view {0}" + v);
	  					Console.WriteLine("Group {0} has {1} members", local, shardGroup[local].GetView().GetSize());
	  				}
	  				
	  				if (shardGroup[local].GetView().GetSize() == shardSize) {
	  					groupJoin[local] = true;
	  				}
	  				
	  				bool isAll = true;
	  				for (int j = 0; j < shardSize; j++) {
	  					if (groupJoin[j] == false) {
	  						isAll = false;
	  						break;
	  					}
	  				}
	  				
	  				if (isAll) {
	  					allJoin = true;
	  					if (Parameter.isVerbose) {
	  						Console.WriteLine("All the members have joined!");
	  					}
	  				}
	  			};
	  		}
	  		
	  		for (int i = 0; i < shardSize; i++) {
	  			shardGroup[i].Join();
	  		}
	  	}		
		
	}
	
	//Parameters class
	class Parameter {
		public static bool isVerbose = false;        //Is verbosely print out
		public static int portNum = 1234;            //Default port number 1234
		public static int memPortNum = 9999;         //Default memcached port number 9999

	  	//Talk to local memcached
	  	public static string talkToMem(string command, int commandType) {
	  		TcpClient client = new TcpClient();
	  		string line = "";
	  		string reply = "";
	  		
	  		try {
	  			client.Connect("localhost", memPortNum);
	  			NetworkStream ns = client.GetStream();
	  			byte[] sendBytes = Encoding.ASCII.GetBytes(command);
	  			StreamReader reader = new StreamReader(client.GetStream(), System.Text.Encoding.ASCII);
	  			
	  			//Send command to local memcached
	  			if (ns.CanRead && ns.CanWrite) {
	  				ns.Write(sendBytes, 0, sendBytes.Length);
	  			}
	  			
	  			if (commandType == IsisServer.INSERT) {
  					line = reader.ReadLine();
  					reply = line;
  					client.Close();
	  			} else if (commandType == IsisServer.GET) {
  					while ((line = reader.ReadLine()) != null) {
  						reply += line;
  						reply += "\r\n";
  						
  						if (line == "END") {
  							break;
  						}
  					}
  					client.Close();
	  			}
	  		} catch (Exception e) {
	  			Console.WriteLine("Exception in talking to memcached!");
	  			if (client != null) {
	  				client.Close();
	  			}
	  		}	
	  			return reply;
	  	}
	  	
	  	//Print out usage
	  	public static void printUsage() {
	  		Console.WriteLine("Usage:");
	  		Console.Write("-p: listening port number. Default: 1234\n" + 
	  					  "-m: memcached port number. Default: 9999\n" +
	  		              "-v: is verbose. Default: no\n" + 
	  		              "-n: total node number. Has to be specified\n" + 
	  		              "-r: my rank. Has to be specified\n" +
	  		              "-t: timeout for ISIS query. Default: 15 sec" +
	  		              "-s: shard size. Has to be specifed\n");
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

