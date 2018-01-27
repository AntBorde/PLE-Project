package bigdata;

import org.apache.hadoop.util.ProgramDriver;

public class App {
	
	public static double sizeTile = 0;
	public static final int METER_PER_DEG_EQUATEUR = 111319; //wikipedia
	public static double latTile = 0;
	
	
    public static void main( String[] args )
    {
    	setSize(1000);
    	System.out.println(latTile);
    	
    	ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			System.out.println("test");
			pgd.addClass("Tile", bigdata.TileReduce.class, "map reduce un zoom de la carte");
			System.out.println("test1");
			//pgd.addClass("Histogramme", bigdata.Histogramme.class, "fabrique l'histogramme");
			//pgd.addClass("resume", BigData.worldpop.ResumeCities.class, "Agregate cities according to their population");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
    	
    }
    
    
    public static double getSize() {
		return sizeTile;
	}
	
	public static void setSize(int size) {
		sizeTile=size;
		latTile = sizeTile/METER_PER_DEG_EQUATEUR;
	}
	
	public static double getLatTile() {
		return latTile;
	}
	
	
}
