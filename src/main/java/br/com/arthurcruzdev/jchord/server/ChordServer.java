package br.com.arthurcruzdev.jchord.server;

import br.com.arthurcruzdev.jchord.interfaces.IKeyNodeIdentifierMaker;
import br.com.arthurcruzdev.jchord.thrift.Chord;
import br.com.arthurcruzdev.jchord.handler.ChordHandler;
import br.com.arthurcruzdev.jchord.thrift.NodeInfo;
import br.com.arthurcruzdev.jchord.utils.DefaultKeyNoteIdentifierMaker;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ChordServer {
    private static final Logger log = LoggerFactory.getLogger(ChordServer.class);
    private static ChordHandler chordHandler;
    private static Chord.Processor chordProcessor;
    private static final NodeInfo thisServerNode = new NodeInfo();
    private static boolean isInitialized = false;
    private static boolean isRootNode = false;
    public static volatile ChordServer instance;
    private final IKeyNodeIdentifierMaker keyNodeIdentifierMaker;

    private ChordServer(final int port, final boolean isRootNode, final IKeyNodeIdentifierMaker keyNodeIdentifierMaker){
        if(port < 0 || port > 65535){
            throw new IllegalArgumentException("Failed to create JChord server due to requested port number being invalid.");
        }
        thisServerNode.port = port;
        ChordServer.isRootNode = isRootNode;
        this.keyNodeIdentifierMaker = keyNodeIdentifierMaker;
    }

    public void init(){
        if(isInitialized){
            log.info("JChord Server already initialized");
            return;
        }
        log.info("Initializing JChord Server...");
        Socket socket = new Socket();
        try{
            socket.connect(new InetSocketAddress("google.com", 80));
            thisServerNode.ip = socket.getLocalAddress().getHostAddress().toString();
            log.info("Found local IP Address: {}", thisServerNode.ip);
            thisServerNode.id = this.keyNodeIdentifierMaker.getIdentifier(thisServerNode);
            log.info("Current server's defined ID: {}", thisServerNode.id);
        }catch(IOException ioe){
            throw new IllegalStateException("Failed to initialize JChord server due to not being able to define current instance's IP Address");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to initialize JChord server due to unavailability of SHA-256 hash algorithm in the current machine");
        }
        try{
            chordHandler = new ChordHandler();
            chordProcessor = new Chord.Processor(chordHandler);
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(thisServerNode.port);
            TServer server = new TNonblockingServer(
                    new TNonblockingServer
                            .Args(serverTransport)
                            .processor(chordProcessor)
                            .protocolFactory(new TCompactProtocol.Factory())
            );
            server.serve();
            log.info("JChord Server Successfully Initialized");
        } catch (TTransportException e) {
            log.error("Failed to initialize JChord server at port {} due to: ", thisServerNode.port ,e);
            throw new RuntimeException(e);
        }
    }

    public static ChordServer getInstance(final int port, final boolean isRootNode){
        if(instance != null){
            return instance;
        }
        synchronized (ChordServer.class){
            if(instance == null){
                instance = new ChordServer(port, isRootNode, new DefaultKeyNoteIdentifierMaker());
            }
            return instance;
        }
    }

}
