import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Arrays;

public class InversionCE {
	public static long CountInv(int[] array) {
		if(array.length <=1) {
			return 0;
		}
		long x=0,y=0,z=0,inv=0;
		int mid = (array.length)/2;
		//System.out.println("mid is:"+mid);
		//int[] left = Arrays.copyOfRange(array, start, mid);
		//int[] right = Arrays.copyOfRange(array, mid, end);
		int left[]=new int[mid];
		int right[]=new int [array.length-mid];

		for (int k=0;k<mid;k++){
		    left[k]=array[k];
		}
		for (int k=mid;k<array.length;k++){
		    right[k-mid]=array[k];
		}
		/*System.out.println("left array");
		for(int a=0;a<left.length;a++) {
			System.out.print(left[a]+",");
		}
		System.out.println();
		System.out.println("right array");
		for(int a=0;a<right.length;a++) {
			System.out.print(right[a]+",");
		}*/
		x += CountInv(left);
		y += CountInv(right);
		//System.out.println("mid+1:"+mid+1+" end:"+end);
		z += CountSplitInv(array,left,right);
		//System.out.println("x:"+x+" y:"+y+" z:"+z);
		/*System.out.println("output array");
		for(int a=0;a<array.length;a++) {
			System.out.print(array[a]+",");
		}*/
		return x+y+z;
	}
	public static int CountSplitInv(int[] array, int[] left,int[] right) {
		int i=0,j=0,k=0,count=0;
		//System.out.println("left array length is:"+left.length+" right array length is:"+right.length);
		while (i<left.length && j<right.length) {
			//System.out.println("i is:"+i+" elment is:"+left[i]+" j is:"+j+" element is:"+right[j]);
			if(left[i] <= right[j]) {
				array[k] = left[i];
				i++;
			}
			//System.out.println("i..... is:"+i+" j..... is:"+j);
			else if(left[i] > right[j]) {
				array[k] = right[j];
				j++;
				count += left.length-i;
			}
			k++;
		}
		while( i < left.length) {
			array[k] = left[i];
			k++;
			i++;
		}
		while( j < right.length) {
			array[k] = right[j];
			k++;
			j++;
		}
		
		//System.out.println("count is:"+count);
		return count;
	}
	public static void main(String[] args) throws IOException {
		String fileName = "C:\\Users\\lavanya\\Desktop\\Matsers\\Design and Algo\\coursera\\InversionInput.txt";
		String line = null;
		//int count=0;

		LineNumberReader  lnr = new LineNumberReader(new FileReader(new File("C:\\Users\\lavanya\\Desktop\\Matsers\\Design and Algo\\coursera\\InversionInput.txt")));
		lnr.skip(Long.MAX_VALUE);
		System.out.println(lnr.getLineNumber() + 1); //Add 1 because line index starts at 0
		// Finally, the LineNumberReader object should be closed to prevent resource leak
		lnr.close();
		int count = lnr.getLineNumber() + 1, cnt=0;
		int input[] = new int[count];
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
            	//System.out.println("value:"+Integer.valueOf(line));
                input[cnt] = Integer.valueOf(line);
            	//System.out.println(line);
                //count++;
                cnt++;
            }   

            // Always close files.
            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        System.out.println("no of inputs:"+count);
		int[] test = {54044,14108,79294,29649,25260,60660,2995,53777,49689};
		/*for(int a=0;a<input.length;a++) {
			System.out.println(input[a]+",");
		}*/
		System.out.println("Inversions is:"+CountInv(input));
	}
}