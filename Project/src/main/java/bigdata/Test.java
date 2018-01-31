package bigdata;

import scala.Tuple2;

public class Test {

	public static void main(String[] args) {
		//double x = 52.0008546;
		//x = x/0.001;
		//System.out.println(LocGPS.correctionLongitude(x));
		//Math.floor(x);
		//double x2 = 
		/*System.out.println("hello");
		int x = 5187;
		int y = 649;
		LocGPS loc = new LocGPS();
		loc.setH(2404);
		loc.generateLocTileCenter(x, y ,200);
		System.out.println(loc.toString());
		double z =35.60031081845867;
		System.out.println(z);
		float c = (float)z;
		System.out.println(c);
		
		//System.out.println(LocGPS.getKey(52, 52, 200));
		System.out.println(Const.getLatTile());
		System.out.println(Const.getLatTile(200));*/
		
		/*double size = 200;
		int nbReduce = 0;
		for (int i = 0; i < 5; i++) {
			System.out.println(size*Math.pow(2,nbReduce));
			nbReduce++;
		}*/
		
		/*double x = 52.0008546;
		double y = 52.0008546;
	
		LocGPS loc = new LocGPS(x,y,2000,(short)1);
		Tuple2<Integer,Integer> t = loc.getFirstCanvasKey();
		System.out.println(t);
		t= loc.getFirstKey();
		System.out.println(t);
		System.out.println(Const.DEGRE_PER_POINT_HGT_FORMAT);
		double test = ((double)1)/2;
		System.out.println(test);*/
		
		double x = 135.37385512;
		double y = 187.357202000;
		LocGPS loc = new LocGPS(x,y,2000,(short)0);
		System.out.println(loc.getKey(200));

	}

}
