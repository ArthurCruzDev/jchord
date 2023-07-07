package br.com.arthurcruzdev;

import br.com.arthurcruzdev.jchord.server.JChordServer;
import br.com.arthurcruzdev.jchord.thrift.NodeInfo;

public class Main {
    public static void main(String[] args) {
        String portEnv = System.getenv("JCHORD_PORT");
        String rootNodeHostEnv = System.getenv("JCHORD_ROOT_NODE_HOST");
        String rootNodeHostPortEnv = System.getenv("JCHORD_ROOT_NODE_PORT");
        int port = portEnv != null && !portEnv.isBlank() ? Integer.valueOf(portEnv) : 9090;
        int rootNodeHostPort = rootNodeHostPortEnv != null && !rootNodeHostPortEnv.isBlank() ? Integer.valueOf(rootNodeHostPortEnv) : 9090;

        JChordServer jChordServer = JChordServer.getInstance(port, rootNodeHostEnv != null ? new NodeInfo(-1l, rootNodeHostEnv, rootNodeHostPort) : null);
        jChordServer.init();
    }
}