package br.com.arthurcruzdev.jchord.server;

import br.com.arthurcruzdev.jchord.interfaces.IIPDiscoveryService;
import br.com.arthurcruzdev.jchord.interfaces.IKeyNodeIdentifierMaker;
import br.com.arthurcruzdev.jchord.thrift.Chord;
import br.com.arthurcruzdev.jchord.handler.JChordHandler;
import br.com.arthurcruzdev.jchord.thrift.NodeInfo;
import br.com.arthurcruzdev.jchord.utils.DefaultIPDiscoveryService;
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
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class JChordServer {
    private static final Logger log = LoggerFactory.getLogger(JChordServer.class);
    private static boolean isInitialized = false;
    private static boolean isRootNode = false;
    public static volatile JChordServer instance;

    private final NodeInfo thisServerNode = new NodeInfo();
    private final List<NodeInfo> fingerTable = new ArrayList<>(IKeyNodeIdentifierMaker.NUM_BITS);
    private final IKeyNodeIdentifierMaker keyNodeIdentifierMaker;
    private final IIPDiscoveryService ipDiscoveryService;

    private JChordServer(final int port, final boolean isRootNode, final IKeyNodeIdentifierMaker keyNodeIdentifierMaker, final IIPDiscoveryService ipDiscoveryService){
        if(port < 0 || port > 65535){
            throw new IllegalArgumentException("Failed to create JChord server due to requested port number being invalid.");
        }
        thisServerNode.port = port;
        JChordServer.isRootNode = isRootNode;
        this.keyNodeIdentifierMaker = keyNodeIdentifierMaker;
        this.ipDiscoveryService = ipDiscoveryService;
    }

    public void init(){
        if(isInitialized){
            log.info("JChord Server already initialized");
            return;
        }
        log.info("Initializing JChord Server...");
        try{
            thisServerNode.ip = this.ipDiscoveryService.discoverIP();
            log.info("Found local IP Address: {}", thisServerNode.ip);
            thisServerNode.id = this.keyNodeIdentifierMaker.getIdentifier(thisServerNode);
            log.info("Current server's defined ID: {}", thisServerNode.id);
        }catch(IOException ioe){
            throw new IllegalStateException("Failed to initialize JChord server due to not being able to define current instance's IP Address");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to initialize JChord server due to unavailability of SHA-256 hash algorithm in the current machine");
        }
        try{
            JChordHandler jChordHandler = new JChordHandler();
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(thisServerNode.port);
            TServer server = new TNonblockingServer(
                    new TNonblockingServer
                            .Args(serverTransport)
                            .processor(new Chord.Processor(jChordHandler))
                            .protocolFactory(new TCompactProtocol.Factory())
            );
            initializeFingerTable();
            if(!isRootNode){

            }
            log.info("JChord Server Successfully Initialized");
            isInitialized = true;
            log.info("JChord Server available on port: {}", thisServerNode.port);
            server.serve();
        } catch (TTransportException e) {
            log.error("Failed to initialize JChord server at port {} due to: ", thisServerNode.port ,e);
            throw new RuntimeException(e);
        }
    }

    private void initializeFingerTable(){
        for(int i = 0; i < IKeyNodeIdentifierMaker.NUM_BITS; i++){
            NodeInfo nodeInfo = new NodeInfo();
            nodeInfo.setId(thisServerNode.id);
            nodeInfo.setIp(thisServerNode.ip);
            nodeInfo.setPort(thisServerNode.port);
            fingerTable.set(i, nodeInfo);
        }
    }

    public static JChordServer getInstance(final int port, final boolean isRootNode){
        if(instance != null){
            return instance;
        }
        synchronized (JChordServer.class){
            if(instance == null){
                instance = new JChordServer(port, isRootNode, new DefaultKeyNoteIdentifierMaker(), new DefaultIPDiscoveryService());
            }
            return instance;
        }
    }

}
