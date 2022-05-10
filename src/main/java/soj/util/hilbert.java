package soj.util;
import java.util.*;
import java.math.*;

public class hilbert {

    public class PointH {
        public long x; // X坐标
        public long y; // X坐标
        public PointH() {
        }

        public PointH(long x, long y) {
            this.x = x;
            this.y = y;
        }
    }

        private static void rot(long n, PointH pt, long rx, long ry) {
            if (ry == 0) {
                if (rx == 1) {
                    pt.x = n - 1 - pt.x;
                    pt.y = n - 1 - pt.y;
                }

                //Swap x and y
                long temp = pt.x;
                pt.x = pt.y;
                pt.y = temp;
            }
        }
        //XY坐标到Hilbert代码转换
        public  static  long xy2d(long n, PointH pt) {
            long rx, ry, s, d = 1;
            for (s = n / 2; s > 0; s /= 2) {
                rx = ((pt.x & s) > 0) ? 1 : 0;
                ry = ((pt.y & s) > 0) ? 1 : 0;
                d += s * s * ((3 * rx) ^ ry);
                rot(s, pt, rx, ry);
            }
            return d;
        }

/*        public static void main(String[] args) {
            Scanner sc=new Scanner(System.in);
            int m=sc.nextInt();
            int n =1<<(m);
            long p=sc.nextInt();
            int max = 0;

            long i = 3071453L;
            long j = 10410352L;
            long index = xy2d(n, new Point(j, i));
            System.out.println("(" + j +"," + i + ") = " + index);


//        for (i = 0; i < n; i++) {
//            for (j = 0; j < n; j++) {
//                int index = xy2d(n, new Point(j, i));
//                max = Math.max(max, index);
//                System.out.println("(" + j +"," + i + ") = " + index);
//                if(index==p) {
//                    System.out.println(j+" "+i);
//                }
//            }
//            //System.out.println();
//        }
            System.out.println("Max: " + max);
            //System.out.println(hilbert.xy2d(n, new Point(3,0)));
        }*/


}
