package br.com.arthurcruzdev.jchord.server;

import br.com.arthurcruzdev.jchord.interfaces.IIPDiscoveryService;
import br.com.arthurcruzdev.jchord.interfaces.IKeyNodeIdentifierMaker;
import br.com.arthurcruzdev.jchord.thrift.*;
import br.com.arthurcruzdev.jchord.utils.DefaultIPDiscoveryService;
import br.com.arthurcruzdev.jchord.utils.DefaultKeyNoteIdentifierMaker;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class JChordServer implements Chord.Iface{
    private static final Logger log = LoggerFactory.getLogger(JChordServer.class);
    private static boolean isInitialized = false;
    private static NodeInfo rootNode;
    public static volatile JChordServer instance;

    private final NodeInfo thisServerNode = new NodeInfo();
    private final List<NodeInfo> fingerTable = new ArrayList<>(IKeyNodeIdentifierMaker.NUM_BITS);
    private final IKeyNodeIdentifierMaker keyNodeIdentifierMaker;
    private final IIPDiscoveryService ipDiscoveryService;

    private JChordServer(final int port, final NodeInfo rootNode, final IKeyNodeIdentifierMaker keyNodeIdentifierMaker, final IIPDiscoveryService ipDiscoveryService){
        if(port < 0 || port > 65535){
            throw new IllegalArgumentException("Failed to create JChord server due to requested port number being invalid.");
        }
        thisServerNode.port = port;
        JChordServer.rootNode = rootNode;
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
            TServerTransport serverTransport = new TServerSocket(thisServerNode.port);
            TServer server = new TThreadPoolServer(
                    new TThreadPoolServer
                            .Args(serverTransport)
                            .processor(new Chord.Processor(this))
                            .protocolFactory(new TCompactProtocol.Factory())
            );
            initializeFingerTable();

            if(rootNode == null){

            }
            log.info("JChord Server Successfully Initialized");
            isInitialized = true;
            log.info("JChord Server available on port: {}", thisServerNode.port);
            server.serve();
        } catch (Exception e) {
            log.error("Failed to initialize JChord server at port {} due to: ", thisServerNode.port ,e);
            throw new RuntimeException(e);
        }
    }

    private void initializeFingerTable(){
        log.info("Initializing JChord Server's routing table");
        if(rootNode == null){
            for(int i = 0; i <= IKeyNodeIdentifierMaker.NUM_BITS; i++){
                NodeInfo nodeInfo = new NodeInfo();
                nodeInfo.setId(thisServerNode.id);
                nodeInfo.setIp(thisServerNode.ip);
                nodeInfo.setPort(thisServerNode.port);
                fingerTable.add(nodeInfo);
            }
        }else{
            TTransport transport = null;
            NodeInfo successor = null;
            try{
                try{
                    transport = new TSocket(rootNode.getIp(),rootNode.getPort());
                    transport.open();
                } catch (TTransportException e) {
                    log.error("Failed to create transport to node on {}:{}", rootNode.getIp(), rootNode.getPort());
                    throw new RuntimeException(e);
                }

                TProtocol protocol = new TCompactProtocol(transport);
                Chord.Client client = new Chord.Client(protocol);
                try {
                    successor  = client.findSuccessor(this.thisServerNode.id);
                } catch (TException e) {
                    log.error("Failed to find this JChord Server's successor while initializing routing table");
                    throw new RuntimeException(e);
                }
            }finally{
                if(transport != null){
                    transport.close();
                }
            }

            try{
                try{
                    transport = new TSocket(successor.getIp(),successor.getPort());
                    transport.open();
                } catch (TTransportException e) {
                    log.error("Failed to create transport to successor on {}:{} while initializing routing table", rootNode.getIp(), rootNode.getPort());
                    throw new RuntimeException(e);
                }

                TProtocol protocol = new TCompactProtocol(transport);
                Chord.Client client = new Chord.Client(protocol);
                this.fingerTable.clear();
                try{
                    this.fingerTable.addAll(client.getFingerTable());
                } catch (TException e) {
                    log.error("Failed to retrieve routing table from successor");
                    throw new RuntimeException(e);
                }
            }finally{
                if(transport != null){
                    transport.close();
                }
            }
            int lastUsefulIndex = 1;
            for(int i = 1; i <= IKeyNodeIdentifierMaker.NUM_BITS; i++){
                long keyNumber = calculateFingerTableKeyNumberByIndex(this.thisServerNode.id, i);
                if(lastUsefulIndex != -1 && keyNumber < this.fingerTable.get(lastUsefulIndex).getId()){
                    boolean found = false;
                    for(int j = i+1; j < this.fingerTable.size(); j++){
                        if(keyNumber <= this.fingerTable.get(j).getId()){
                            this.fingerTable.set(i, this.fingerTable.get(j));
                            lastUsefulIndex = j;
                            found = true;
                            break;
                        }
                    }
                    if(!found){
                        lastUsefulIndex = -1;
                    }
                }else{
                    try{
                        try{
                            transport = new TSocket(this.fingerTable.get(this.fingerTable.size() - 1).getIp(),
                                                    this.fingerTable.get(this.fingerTable.size() - 1).getPort()
                                            );
                            transport.open();
                        } catch (TTransportException e) {
                            log.error("Failed to create transport to successor on {}:{} while initializing routing table", rootNode.getIp(), rootNode.getPort());
                            throw new RuntimeException(e);
                        }

                        TProtocol protocol = new TCompactProtocol(transport);
                        Chord.Client client = new Chord.Client(protocol);
                        try{
                            this.fingerTable.set(i, client.findSuccessor(keyNumber));
                        } catch (TException e) {
                            log.error("Failed to retrieve successor of id {}", keyNumber);
                            throw new RuntimeException(e);
                        }
                    }finally{
                        if(transport != null){
                            transport.close();
                        }
                    }
                }
            }
        }
        this.printFingerTable();
        log.info("Successfully initialized JChord Server's routing table");
    }

    private void printFingerTable(){
        StringBuffer stringBuffer = new StringBuffer("\n");
        for(int i = 1; i < this.fingerTable.size();i++){
            NodeInfo nodeInfo = this.fingerTable.get(i);
            stringBuffer
                    .append(i)
                    .append(" | ")
                    .append(calculateFingerTableKeyNumberByIndex(this.thisServerNode.id, i))
                    .append(" | ")
                    .append(nodeInfo.getId())
                    .append(" -> ")
                    .append(nodeInfo.ip)
                    .append(":")
                    .append(nodeInfo.port)
                    .append("\n");
        }
        log.info("Current JChord Server's routing table: {}", stringBuffer);
    }
    private long calculateFingerTableKeyNumberByIndex(long baseId, int index){
        return ( baseId + (long) Math.pow(2.0, index - 1.0) ) % (long) Math.pow(2.0, IKeyNodeIdentifierMaker.NUM_BITS);
    }
    public static JChordServer getInstance(final int port, final NodeInfo rootNode){
        if(instance != null){
            return instance;
        }
        synchronized (JChordServer.class){
            if(instance == null){
                instance = new JChordServer(port, rootNode, new DefaultKeyNoteIdentifierMaker(), new DefaultIPDiscoveryService());
            }
            return instance;
        }
    }

    @Override
    public void join(NodeInfo n) throws UnableToJoinChordException, TException {

    }

    @Override
    public NodeInfo findSuccessor(long id) throws UnableToFindSuccessorException, TException {
        NodeInfo aux = this.findPredecessor(id);
        if(aux.equals(this.thisServerNode)){
            return this.fingerTable.get(1);
        }else{
            TTransport transport = null;
            try{
                try{
                    transport = new TSocket(aux.getIp(),aux.getPort());
                    transport.open();
                } catch (TTransportException e) {
                    log.error("Failed to create transport to aux on {}:{} while finding successor", rootNode.getIp(), rootNode.getPort());
                    throw new RuntimeException(e);
                }

                TProtocol protocol = new TCompactProtocol(transport);
                Chord.Client client = new Chord.Client(protocol);
                aux = client.getSuccessor();
            }finally{
                if(transport != null){
                    transport.close();
                }
            }
            return aux;
        }
    }

    @Override
    public NodeInfo findPredecessor(long id) throws UnableToFindPredecessorException, TException {
        NodeInfo aux = this.thisServerNode;
        NodeInfo successor = this.fingerTable.get(1);

        while(id <= aux.getId() && id > successor.getId()){
            aux = this.closestPrecedingFinger(id);
            TTransport transport = null;
            try{
                try{
                    transport = new TSocket(aux.getIp(),aux.getPort());
                    transport.open();
                } catch (TTransportException e) {
                    log.error("Failed to create transport to aux on {}:{} while finding predecessor", rootNode.getIp(), rootNode.getPort());
                    throw new RuntimeException(e);
                }

                TProtocol protocol = new TCompactProtocol(transport);
                Chord.Client client = new Chord.Client(protocol);
                if(aux.equals(this.thisServerNode)) {
                    aux = this.closestPrecedingFinger(id);
                }else{
                    aux = client.closestPrecedingFinger(id);
                }
                successor = client.getSuccessor();
            }finally{
                if(transport != null){
                    transport.close();
                }
            }
        }
        return aux;
    }

    @Override
    public NodeInfo closestPrecedingFinger(long id) throws UnableToFindClosestPrecedingFingerException, TException {
        for(int i = IKeyNodeIdentifierMaker.NUM_BITS; i > 0; i--){
            if(calculateFingerTableKeyNumberByIndex(this.thisServerNode.id, i) < id){
                return this.fingerTable.get(i);
            }
        }
        return this.thisServerNode;
    }

    @Override
    public List<ByteBuffer> transferKeys(NodeInfo n) throws UnableToTransferKeysException, TException {
        return null;
    }

    @Override
    public void notify(NodeInfo n) throws UnableToNotifyException, TException {

    }

    @Override
    public List<NodeInfo> getFingerTable() throws TException {
        return this.fingerTable;
    }

    @Override
    public NodeInfo getSuccessor() throws TException {
        return this.fingerTable.get(1);
    }
}
