package utils;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class Test_NumberOfProcessors_Test {
	@Test
    public void test_number_of_processors() throws IOException {
		
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of available processors: " + cores);
	}

}
