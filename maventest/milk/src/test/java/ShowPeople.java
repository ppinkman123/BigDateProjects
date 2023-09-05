import org.junit.Test;
import static junit.framework.Assert.*;

public class ShowPeople {
    @Test
    public void show(){
        People p = new People();
        int age = p.getAge();
        assertEquals(10,age);
    }
}
