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
}

