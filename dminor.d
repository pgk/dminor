#!/usr/bin/env rdmd

import std.conv;
import std.stdio;
import std.string;
import std.socket;
import core.sys.posix.signal;
import std.c.process;
import std.concurrency;
import core.thread;
import std.random;
import std.datetime;


static immutable HASH_FILE = "dict.dmindb";
static immutable MAX_LOCAL_SIZE_MB = 16;
static immutable BUFFER_SIZE = 128;
static immutable PORT = 18181;
static immutable MBIT_LENGTH = 4;


static bool KEEP_GOING = true;


struct NodeEntry
{
	string ip;
	uint port;
	string id;
	bool master;
}


class ShareCached
{
	string[string] dict;
	KDProtocol[Socket] socketHandlers;
	NodeEntry myinfo;

	this(string host, ushort port)
	{
		myinfo.ip = host;
		myinfo.port = to!uint(port);
	}

	this(string host, ushort port, string[string] otherdict)
	{
		this(host, port);
		dict = otherdict;
	}

	void set(string key, string value)
	{
		dict[key] = value;
	}

	string get(string key)
	{
		string *s;
		s = (key in dict);
		return *s;
	}

	void addNode(Socket conn)
	{
		writeln("Got connection", conn);
		socketHandlers[conn] = new KDProtocol();
		socketHandlers[conn].makeConnection(conn, this);
	}

	void addNodeEntry(NodeEntry ne)
	{
	}

	void checkWrites(SocketSet writeSockets)
	{
		foreach (s; socketHandlers.keys) {
			if (writeSockets.isSet(s) != 0) {

				ubyte[BUFFER_SIZE] buffer;
				auto dataLength = s.receive(buffer);

				if (dataLength == 0) {
					socketHandlers[s].connectionLost();
					socketHandlers[s].transport = null;
					closeSocket(s);

					socketHandlers[s] = null;
					socketHandlers.remove(s);

				} else if (dataLength == -1) {
					// pass
				} else {
					socketHandlers[s].dataReceived(buffer, dataLength);
				}
			}
		}
	}

	void onClose()
	{
		foreach (s; socketHandlers.keys) {
			socketHandlers[s].connectionLost();
			closeSocket(s);
		}
	}
}


class Protocol
{
	Socket transport;
	ShareCached factory;


	void makeConnection(Socket s, ShareCached f)
	{
		transport = s;
		factory = f;
		connectionMade(s);
	}

	void disconnect()
	{
		log("disconnecting socket");
		closeSocket(transport);
		transport = null;
	}

	void connectionMade(Socket s)
	{
		log("I made a connection!");
	}

	void dataReceived(ubyte [] data, ulong length)
	{
		auto sent = transport.send(cast(ubyte [])"got it\r\n");
	}

	void connectionLost()
	{
		log("connection with socket lost");
	}

	~this()
	{
		factory = null;
		log("killilng Protocol");
	}
}


class LineProtocol : Protocol
{
	char[] lineData;

	override void dataReceived(ubyte [] data, ulong length)
	{
		lineData ~= cast(char [])data[0..length];
		if (lineData[$-1] == '\n') {
			lineReceived(chomp(lineData.idup));
			lineData = [];
		}
	}

	void lineReceived(string line)
	{
	}

	void sendCommand(string token, in string[] args ...)
	{
		sendString(token ~ " " ~ args.join(" "));
	}

	ptrdiff_t sendString(string s)
	{
		auto ss = s ~ "\r\n";
		return transport.send(cast(ubyte [])ss);
	}
}


class KDProtocol : LineProtocol
{
	override void lineReceived(string line)
	{
		auto commands = line.split(" ");
		auto seq = commands[0];
		auto type = commands[0];

		switch (type) {
			case "GET":
				auto value = factory.get(commands[1]);
				if (value != null) {
					sendCommand(value);
				} else {
					sendCommand("nil");
				}
				break;
			case "PING":
				sendCommand("PONG");
				break;
			case "SET":
				factory.set(commands[1], commands[2]);
				sendCommand("OK");
				break;
			default:
				sendCommand("INVALID_OPERATION");
				break;
		}
	}
}


void closeSocket(Socket s)
{
	s.shutdown(SocketShutdown.BOTH);
	s.close();
}


TcpSocket listenTCP(const(char[]) addr, ushort port)
{
	try {
		auto address = parseAddress(addr, port);
		auto sock = new TcpSocket(AddressFamily.INET);
		sock.bind(address);
		sock.listen(1);
		log("will start accepting");
		return sock;
	} catch (SocketOSException e) {
		writeln(e);
	}

	return null;
}


void log(string what)
{
	auto now = Clock.currTime();
	writeln(format("[%s][dminor] %s", now.toISOExtString(), what));
}


extern (C) nothrow @nogc @system void handleSignal(int sigNum)
{
	printf("got signal %d, will stop now\n", sigNum);
	KEEP_GOING = false;
}


void main()
{
	log("dminor is initializing");
	log("opening " ~ HASH_FILE);

	auto localhost = "127.0.0.1";

	signal(SIGTERM, &handleSignal);
	signal(SIGINT,  &handleSignal);
	signal(SIGKILL, &handleSignal);
	signal(SIGHUP,  &handleSignal);


	auto sock = listenTCP(localhost, PORT);
	if (sock is null) {
		writeln("error connecting to ", localhost, PORT);
		return;
	}

	sock.blocking = false;

	scope(exit)closeSocket(sock);

	log(format("listening at tcp://%s:%d", localhost, PORT));
	
	auto cache = new ShareCached(localhost, PORT);

	auto readSockets = new SocketSet();
	auto writeSockets = new SocketSet();
	auto errorSockets = new SocketSet();

	while (KEEP_GOING) {
		readSockets.reset();
		writeSockets.reset();
		errorSockets.reset();

		readSockets.add(sock);
		auto newConnection = Socket.select(readSockets, null, null, 50.msecs);
		//writeln(newConnection);
		if (newConnection > 0) {
			auto conn = sock.accept();
			cache.addNode(conn);
		}

		foreach (s; cache.socketHandlers.keys) {
			writeSockets.add(s);
		}

		auto writes = Socket.select(null, writeSockets, null, 50.msecs);
		if (writes > 0) {
			cache.checkWrites(writeSockets);
		}
	}

	log("killilng Main");
	closeSocket(sock);
	cache.onClose();
}
