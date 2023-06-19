package br.com.arthurcruzdev;

import br.com.arthurcruzdev.thrift.Chord;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TSaslNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ChordServer {
    private static final Logger log = LoggerFactory.getLogger(ChordServer.class);
    private static ChordHandler chordHandler;
    private static Chord.Processor chordProcessor;

    public ChordServer(){

    }

    public void init(){
        log.info("Initializing JChord Server...");
        try{
            chordHandler = new ChordHandler();
            chordProcessor = new Chord.Processor(chordHandler);
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(9090);
            TServer server = new TNonblockingServer(
                    new TNonblockingServer
                            .Args(serverTransport)
                            .processor(chordProcessor)
                            .protocolFactory(new TCompactProtocol.Factory())
            );
            server.serve();
        } catch (TTransportException e) {
            log.error("Failed to initialize JChord server at port 9090 due to: ", e);
            throw new RuntimeException(e);
        }
    }
}
