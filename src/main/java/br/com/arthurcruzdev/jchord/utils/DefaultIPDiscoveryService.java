package br.com.arthurcruzdev.jchord.utils;

import br.com.arthurcruzdev.jchord.interfaces.IIPDiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class DefaultIPDiscoveryService implements IIPDiscoveryService {
    private static final Logger log = LoggerFactory.getLogger(IIPDiscoveryService.class);
    @Override
    public String discoverIP() throws IOException {
        Socket socket = null;
        String discoveredIP;
        try{
            socket = new Socket();
            socket.connect(new InetSocketAddress("google.com", 80));
            discoveredIP = socket.getLocalAddress().getHostAddress();
        }finally{
            if(socket != null && !socket.isClosed()){
                try{
                    socket.close();
                }catch(IOException ioe){
                    log.error("Failed to close socket used to discover local IP address");
                }
            }
        }
        return discoveredIP;
    }
}
