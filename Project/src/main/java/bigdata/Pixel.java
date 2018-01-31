package bigdata;

import java.io.Serializable;

public class Pixel implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	int x;
	int y;
	int value;
	
	
	public Pixel() {
		
	}
	
	
	public Pixel(int x, int y, int h) {
		this.x=x;
		this.y=y;
		this.value=value;
	}
	
	
	public int getX() {
		return x;
	}
	
	public void setX(int x) {
		this.x = x;
	}
	
	public int getY() {
		return y;
	}
	
	public void setY(int y) {
		this.y = y;
	}
	
	public int getValue() {
		return value;
	}
	
	public void setValue(int value) {
		this.value = value;
	}

}
