package br.com.arthurcruzdev;

import br.com.arthurcruzdev.jchord.server.JChordServer;

public class Main {
    public static void main(String[] args) {
        JChordServer jChordServer = JChordServer.getInstance(9090, true);
        jChordServer.init();
    }
}