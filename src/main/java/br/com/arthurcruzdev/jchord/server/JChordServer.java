package br.com.arthurcruzdev.jchord.server;

import br.com.arthurcruzdev.jchord.interfaces.IIPDiscoveryService;
import br.com.arthurcruzdev.jchord.interfaces.IKeyNodeIdentifierMaker;
import br.com.arthurcruzdev.jchord.thrift.*;
import br.com.arthurcruzdev.jchord.utils.DefaultIPDiscoveryService;
import br.com.arthurcruzdev.jchord.utils.DefaultKeyNoteIdentifierMaker;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JChordServer implements Chord.Iface{
    private static final Logger log = LoggerFactory.getLogger(JChordServer.class);
    private static final TProtocolFactory T_PROTOCOL_FACTORY = new TCompactProtocol.Factory();
    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);
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
                            .protocolFactory(T_PROTOCOL_FACTORY)
            );
            join();
            startStabilizationProtocol();
            log.info("JChord Server Successfully Initialized");
            isInitialized = true;
            log.info("JChord Server available on port: {}", thisServerNode.port);
            server.serve();
        } catch (Exception e) {
            log.error("Failed to initialize JChord server at port {} due to: ", thisServerNode.port ,e);
            throw new RuntimeException(e);
        }
    }

    private void startStabilizationProtocol(){
        scheduledExecutorService.scheduleWithFixedDelay(()->{
            log.info("Running stabilization protocol on JChord Server...");
            NodeInfo aux = remoteGetPredecessor(fingerTable.get(1));
            if(aux != null &&
                    isNumberInRange(thisServerNode.getId(), false,
                            fingerTable.get(1).getId(), false, aux.getId(),
                            IKeyNodeIdentifierMaker.NUM_BITS
                    )
            ){
                fingerTable.set(1, aux);
            }
            remoteNotify(fingerTable.get(1), thisServerNode);
            printFingerTable();
        }, 10, 10, TimeUnit.SECONDS);
    }

    private void initializeFingerTable(){
        log.info("Initializing JChord Server's routing table");

        for(int i = 0; i <= IKeyNodeIdentifierMaker.NUM_BITS; i++){
            if(i == 0){
                fingerTable.add(null);
                continue;
            }
            NodeInfo nodeInfo = new NodeInfo();
            nodeInfo.setId(thisServerNode.id);
            nodeInfo.setIp(thisServerNode.ip);
            nodeInfo.setPort(thisServerNode.port);
            fingerTable.add(nodeInfo);
        }
        this.printFingerTable();
        log.info("Successfully initialized JChord Server's routing table");
    }

    private void printFingerTable(){
        StringBuffer stringBuffer = new StringBuffer("\nIndex\t\t|KeyNumber\t|NodeId\t\t|NodeIP\t\t\t\t|NodePort\n");
        for(int i = 0; i < this.fingerTable.size();i++){
            NodeInfo nodeInfo = this.fingerTable.get(i);
            stringBuffer
                    .append(i)
                    .append("\t\t| ")
                    .append(calculateFingerTableKeyNumberByIndex(this.thisServerNode.id, i))
                    .append("\t\t| ")
                    .append(nodeInfo != null ? nodeInfo.getId() : "NULL")
                    .append("\t\t| ")
                    .append(nodeInfo != null ?  nodeInfo.ip : "NULL\t\t\t")
                    .append("\t| ")
                    .append(nodeInfo != null ? nodeInfo.port: "NULL")
                    .append(i == 0 ? "\t| *Predecessor\n" : "\n");
        }
        log.info("Current JChord Server's routing table: {}", stringBuffer);
    }
    private long calculateFingerTableKeyNumberByIndex(long baseId, int index){
        return ( baseId + (long) Math.pow(2.0, index - 1.0) ) % (long) Math.pow(2.0, IKeyNodeIdentifierMaker.NUM_BITS);
    }
    private void join(){
        this.initializeFingerTable();
        if(rootNode != null){
            this.fingerTable.set(1, remoteFindSuccessor(rootNode, this.thisServerNode.getId()));
        }
        this.printFingerTable();
    }

    private NodeInfo remoteFindSuccessor(NodeInfo node, long id){
        if(node.equals(this.thisServerNode)){
            try{
                this.findSuccessor(id);
            }catch(TException e){
                log.error("Failed to find sucessor for id: {}", id);
                throw new RuntimeException(e);
            }
        }
        return remoteFindSuccessor(node.getIp(), node.getPort(), id);
    }
    private NodeInfo remoteFindSuccessor(String ip, int port, long id){
        TTransport transport = null;
        NodeInfo successor = null;
        try{
            try{
                transport = new TSocket(ip, port);
                transport.open();
            } catch (TTransportException e) {
                log.error("Failed to create transport to node on {}:{}", ip, port);
                throw new RuntimeException(e);
            }

            TProtocol protocol = T_PROTOCOL_FACTORY.getProtocol(transport);
            Chord.Client client = new Chord.Client(protocol);
            try {
                successor  = client.findSuccessor(id);

            } catch (TException e) {
                log.error("Failed to find this JChord Server's successor while initializing routing table");
                throw new RuntimeException(e);
            }
        }catch(Exception e){
            log.error("{}", e);
        }finally{
            if(transport != null){
                transport.close();
            }
        }
        return successor;
    }

    private List<NodeInfo> remoteGetFingerTable(NodeInfo node){
        if(node.equals(this.thisServerNode)){
            return this.fingerTable;
        }
        return remoteGetFingerTable(node.getIp(), node.getPort());
    }
    private List<NodeInfo> remoteGetFingerTable(String ip, int port){
        List<NodeInfo> remoteNodeFingerTable = null;
        TTransport transport = null;
        try{
            try{
                transport = new TSocket(ip, port);
                transport.open();
            } catch (TTransportException e) {
                log.error("Failed to create transport to {}:{} while getting node's routing table", ip, port);
                throw new RuntimeException(e);
            }

            TProtocol protocol = T_PROTOCOL_FACTORY.getProtocol(transport);
            Chord.Client client = new Chord.Client(protocol);

            try{
                remoteNodeFingerTable = client.getFingerTable();

            } catch (TException e) {
                log.error("Failed to get remote JChord Server's routing table");
                throw new RuntimeException(e);
            }
        }finally{
            if(transport != null){
                transport.close();
            }
        }
        return remoteNodeFingerTable;
    }

    private NodeInfo remoteFindClosestPrecedingFinger(NodeInfo node, long id){
        if(node.equals(this.thisServerNode)){
            try{
                return this.closestPrecedingFinger(id);
            }catch(TException e){
                log.error("Failed to find remote JChord Server's closest preceding node for id: {}", id);
                throw new RuntimeException(e);
            }
        }
        return remoteFindClosestPrecedingFinger(node.getIp(), node.getPort(), id);
    }
    private NodeInfo remoteFindClosestPrecedingFinger(String ip, int port, long id){
        TTransport transport = null;
        NodeInfo closestPrecedingFinger = null;
        try{
            try{
                transport = new TSocket(ip, port);
                transport.open();
            } catch (TTransportException e) {
                log.error("Failed to create transport to node on {}:{} while requesting node for closest preceding node for id: {}", ip, port, id);
                throw new RuntimeException(e);
            }

            TProtocol protocol = T_PROTOCOL_FACTORY.getProtocol(transport);
            Chord.Client client = new Chord.Client(protocol);
            try {
                closestPrecedingFinger  = client.closestPrecedingFinger(id);

            } catch (TException e) {
                log.error("Failed to find remote JChord Server's closest preceding node for id: {}", id);
                throw new RuntimeException(e);
            }
        }finally{
            if(transport != null){

                transport.close();
            }
        }
        return closestPrecedingFinger;
    }

    private NodeInfo remoteGetSuccessor(NodeInfo node){
        if(node.equals(this.thisServerNode)){
            return this.fingerTable.get(1);
        }
        return remoteGetSuccessor(node.getIp(), node.getPort());
    }
    private NodeInfo remoteGetSuccessor(String ip, int port){
        TTransport transport = null;
        NodeInfo successor = null;
        try{
            try{
                transport = new TSocket(ip, port);
                transport.open();
            } catch (TTransportException e) {
                log.error("Failed to create transport to node on {}:{} while get node's successor", ip, port);
                throw new RuntimeException(e);
            }

            TProtocol protocol = T_PROTOCOL_FACTORY.getProtocol(transport);
            Chord.Client client = new Chord.Client(protocol);
            try {
                successor  = client.getSuccessor();

            } catch (TException e) {
                log.error("Failed to get remote JChord Server's successor: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }finally{
            if(transport != null){

                transport.close();
            }
        }
        return successor;
    }
    private NodeInfo remoteGetPredecessor(NodeInfo node){
        if(node.equals(this.thisServerNode)){
            return this.fingerTable.get(0);
        }
        return remoteGetPredecessor(node.getIp(), node.getPort());
    }
    private NodeInfo remoteGetPredecessor(String ip, int port){
        TTransport transport = null;
        NodeInfo predecessor = null;
        try{
            try{
                transport = new TSocket(ip, port);
                transport.open();
            } catch (TTransportException e) {
                log.error("Failed to create transport to node on {}:{} while get node's predecessor", ip, port);
                throw new RuntimeException(e);
            }

            TProtocol protocol = T_PROTOCOL_FACTORY.getProtocol(transport);
            Chord.Client client = new Chord.Client(protocol);
            try {
                predecessor  = client.getPredecessor();

            } catch (TException e) {
                log.error("Failed to get remote JChord Server's predecessor: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }finally{
            if(transport != null){

                transport.close();
            }
        }
        return predecessor;
    }

    private void remoteNotify(NodeInfo remoteNode, NodeInfo currentNode){
        if(remoteNode.equals(this.thisServerNode)){
            try{
                this.notify(currentNode);
            }catch (TException e){
                log.error("Failed to notify this JChord Server about possible predecessor");
                throw new RuntimeException(e);
            }
        }else{
            TTransport transport = null;
            try{
                try{
                    transport = new TSocket(remoteNode.getIp(), remoteNode.getPort());
                    transport.open();
                } catch (TTransportException e) {
                    log.error("Failed to create transport to node on {}:{} while notifying", remoteNode.getIp(), remoteNode.getPort());
                    throw new RuntimeException(e);
                }

                TProtocol protocol = T_PROTOCOL_FACTORY.getProtocol(transport);
                Chord.Client client = new Chord.Client(protocol);
                try {
                    client.notify(currentNode);

                } catch (TException e) {
                    log.error("Failed to notify remote JChord Server's about possible predecessor: {}", e.getMessage());
                    throw new RuntimeException(e);
                }
            }finally{
                if(transport != null){
                    transport.close();
                }
            }
        }
    }
    public static boolean isNumberInRange(long start, boolean startInclusive, long end, boolean endInclusive, long number, int numBits){
        number = number % (2L << numBits);
        if(start == end){
            return number != start || startInclusive || endInclusive;
        }else if( (number == start && startInclusive) || (number == end && endInclusive)){
            return true;
        }
        boolean greaterThanStart = startInclusive ? number >= start : number > start;
        boolean lessThanEnd =  endInclusive ? number <= end : number < end;
        if(start > end){
            return  greaterThanStart || ( lessThanEnd  && number >= 0);
        }else{
            return  greaterThanStart &&  lessThanEnd ;
        }
    }

    private void remoteSetPredecessor(NodeInfo remoteNode, NodeInfo newPredecessor){
        if(remoteNode.equals(this.thisServerNode)){
            this.fingerTable.set(0, newPredecessor);
        }
        remoteSetPredecessor(remoteNode.getIp(), remoteNode.getPort(), newPredecessor);
    }
    private void remoteSetPredecessor(String ip, int port, NodeInfo newPredecessor){
        TTransport transport = null;
        try{
            try{
                transport = new TSocket(ip, port);
                transport.open();
            } catch (TTransportException e) {
                log.error("Failed to create transport to node on {}:{} while get node's successor", ip, port);
                throw new RuntimeException(e);
            }

            TProtocol protocol = T_PROTOCOL_FACTORY.getProtocol(transport);
            Chord.Client client = new Chord.Client(protocol);
            try {
                client.setPredecessor(newPredecessor);

            } catch (TException e) {
                log.error("Failed to set remote JChord Server's predecessor: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }finally{
            if(transport != null){

                transport.close();
            }
        }
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
                    log.error("Failed to create transport to aux on {}:{} while finding successor", aux.getIp(), aux.getPort());
                    throw new RuntimeException(e);
                }

                TProtocol protocol = T_PROTOCOL_FACTORY.getProtocol(transport);
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
        NodeInfo auxSuccessor = this.fingerTable.get(1);

        while(!isNumberInRange(aux.getId(), false, auxSuccessor.getId(), true, id, IKeyNodeIdentifierMaker.NUM_BITS)){
            if(aux.equals(this.thisServerNode)){
                aux = this.closestPrecedingFinger(id);
            }else {
                aux = remoteFindClosestPrecedingFinger(aux, id);
            }
            auxSuccessor = remoteGetSuccessor(aux);
        }
        return aux;
    }

    @Override
    public NodeInfo closestPrecedingFinger(long id) throws UnableToFindClosestPrecedingFingerException, TException {
        for(int i = IKeyNodeIdentifierMaker.NUM_BITS; i > 0; i--){
            if(isNumberInRange(
                    thisServerNode.getId(), false, id, false,
                    calculateFingerTableKeyNumberByIndex(this.thisServerNode.id, i),
                    IKeyNodeIdentifierMaker.NUM_BITS
                )){
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
    public void notify(NodeInfo possiblePredecessor) throws UnableToNotifyException, TException {
        if(this.fingerTable.get(0) == null ||
                isNumberInRange(this.fingerTable.get(0).getId(), false,
                        thisServerNode.getId(), false, possiblePredecessor.getId(),
                        IKeyNodeIdentifierMaker.NUM_BITS
                )
        ){
            this.fingerTable.set(0, possiblePredecessor);
        }
    }

    @Override
    public List<NodeInfo> getFingerTable() throws TException {
        return this.fingerTable;
    }

    @Override
    public NodeInfo getSuccessor() throws TException {
        return this.fingerTable.get(1);
    }

    @Override
    public NodeInfo getPredecessor() throws TException {
        return this.fingerTable.get(0);
    }

    @Override
    public void setPredecessor(NodeInfo n) throws TException {
        this.fingerTable.set(0, n);
    }
}
