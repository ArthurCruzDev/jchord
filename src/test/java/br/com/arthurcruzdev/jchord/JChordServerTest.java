package br.com.arthurcruzdev.jchord;

import br.com.arthurcruzdev.jchord.interfaces.IKeyNodeIdentifierMaker;
import br.com.arthurcruzdev.jchord.server.JChordServer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JChordServerTest {

    @Test
    public void shouldCalculateIfNumberIsInRangeCorrectly(){
        assertEquals(false, JChordServer.isNumberInRange(84, false, 34, false, 34, IKeyNodeIdentifierMaker.NUM_BITS));
        assertEquals(false, JChordServer.isNumberInRange(84, false, 34, false, 35, IKeyNodeIdentifierMaker.NUM_BITS));
        assertEquals(false, JChordServer.isNumberInRange(50, false, 60, false, 65, IKeyNodeIdentifierMaker.NUM_BITS));

        assertEquals(true, JChordServer.isNumberInRange(0, false, 2, false, 1, IKeyNodeIdentifierMaker.NUM_BITS));
        assertEquals(true, JChordServer.isNumberInRange(84, false, 34, false, 512, IKeyNodeIdentifierMaker.NUM_BITS));
        assertEquals(true, JChordServer.isNumberInRange(84, false, 34, false, 1024, IKeyNodeIdentifierMaker.NUM_BITS));
    }
}
