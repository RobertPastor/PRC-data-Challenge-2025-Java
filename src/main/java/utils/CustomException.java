package utils;



public class CustomException extends Exception {
	/**
	 * serial generated ID
	 */
	private static final long serialVersionUID = -1346311432020834637L;

	public CustomException(String message) {
		super(" -> " + message);
	}
}


