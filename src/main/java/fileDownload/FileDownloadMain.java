package fileDownload;

import java.io.FileOutputStream;

//Source - https://stackoverflow.com/a
//Posted by Sameer Jadhav
//Retrieved 2025-12-11, License - CC BY-SA 4.0

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


public class FileDownloadMain {


	// Source - https://stackoverflow.com/a
	// Posted by Sameer Jadhav
	// Retrieved 2025-12-11, License - CC BY-SA 4.0

	public static void downloadFile() throws IOException, InterruptedException {
	        String url =  "https://aviationweather.gov/data/schema/openapi.yaml";

	        HttpClient client = HttpClient.newHttpClient();
	        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).build();

	        // Creates new File at provided location user.dir and copies the filename from Content-Disposition
	        	        
	        HttpResponse<Path> response = client.send(request,
	                HttpResponse.BodyHandlers.ofFile(Path.of( System.getProperty("user.dir") ) ) ) ;

	        System.out.println(response.statusCode());

	        if (response.statusCode()  == 200) {
	        	System.out.println("download done correctly");
	        }
	        System.out.println(response.headers());
	        Path path = response.body();
	        System.out.println("Path=" + path); // Absolute Path of downloaded file
	    }
	
	public static void main(String[] args) throws IOException , InterruptedException {
		String dowloadFileFolderPathStr = "C:\\Users\\rober\\eclipse-2025-09\\eclipse-jee-2025-09-R-win32-x86_64\\Data-Challenge-2025\\src\\main\\java\\fileDownload";
				
		Path downloadFolderAsPath = Paths.get(dowloadFileFolderPathStr);
		String fileNameStr = "weather_aviation_weather_openapi.yaml";
		Path fileFullPath = Paths.get(downloadFolderAsPath.toString(), fileNameStr);
		System.out.println(fileFullPath.toString());
		
		System.setProperty("user.dir", fileFullPath.toString());
		FileDownloadMain.downloadFile();
	}

	
}
