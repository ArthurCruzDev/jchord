package br.com.arthurcruzdev;

import br.com.arthurcruzdev.jchord.server.ChordServer;

public class Main {
    public static void main(String[] args) {
        ChordServer chordServer = ChordServer.getInstance(9090, true);
        chordServer.init();
    }
}