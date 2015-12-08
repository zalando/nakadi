package de.zalando.aruha.nakadi.domain;

public class Problem {
	private String message;

	public Problem(final String message) {
		setMessage(message);
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(final String message) {
		this.message = message;
	}
}
