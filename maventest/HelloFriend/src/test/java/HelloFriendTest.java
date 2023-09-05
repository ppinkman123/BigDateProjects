import org.junit.Test;

import static junit.framework.Assert.*;

public class HelloFriendTest {
    @Test
    public void testHelloFriend(){
        HelloFriend helloFriend = new HelloFriend();
        String results = helloFriend.sayHelloToFriend("Maven");
        assertEquals("Hello Maven! I am Idea",results);
    }

}
