package bigdata;

import java.io.Serializable;

public class Canvas implements Serializable {
	

	private static final long serialVersionUID = 1L;
	
	short nbReduce;
	byte[] image;
	
	
	public Canvas() {
	}
	
	public Canvas(short nbReduce, byte[] image) {
		this.nbReduce = nbReduce;
		this.image = image;
	}
	
	
	public short getNbReduce() {
		return nbReduce;
	}
	public void setNbReduce(short nbReduce) {
		this.nbReduce = nbReduce;
	}
	public byte[] getImage() {
		return image;
	}
	public void setImage(byte[] image) {
		this.image = image;
	}


}
