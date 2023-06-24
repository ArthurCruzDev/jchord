package br.com.arthurcruzdev.jchord.utils;

import br.com.arthurcruzdev.jchord.interfaces.IKeyNodeIdentifierMaker;
import br.com.arthurcruzdev.jchord.thrift.NodeInfo;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DefaultKeyNoteIdentifierMaker implements IKeyNodeIdentifierMaker {
    @Override
    public long getIdentifier(NodeInfo nodeInfo) throws NoSuchAlgorithmException {
        return makeIdentifier(nodeInfo.ip.concat(Integer.toString(nodeInfo.port)));
    }

    @Override
    public long getIdentifier(String key) throws NoSuchAlgorithmException{
        return makeIdentifier(key);
    }

    private long makeIdentifier(String key) throws NoSuchAlgorithmException {
        if(key == null || key.trim().isEmpty()){
            throw new IllegalArgumentException("Cannot generate identifier from null or empty key");
        }
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8)), 0, Long.BYTES);
        buffer.flip();
        return (Math.abs(buffer.getLong()) % (1L << NUM_BITS));
    }
}
