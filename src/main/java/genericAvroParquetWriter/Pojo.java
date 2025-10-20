package genericAvroParquetWriter;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class Pojo {

	    private final Map<String, String> aMap;
	    private final long uid;
	    private final long localDateTime;

	    public Pojo() {
	        aMap = new HashMap<>();

	        uid = ThreadLocalRandom.current().nextLong();
	        localDateTime = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	        aMap.put("mapKey", "mapValue");
	    }

	    //getters
	}


