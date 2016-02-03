import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

public class QuickSort {
	public static int comparefirst=0,comparelast=0,comparelast1=0;
	public static void quicksort(int[] array, int left, int right) {
		if(left>=right)
			return;
		int partition = Partition(array,left,right);
		if(left < partition-1)
			quicksort(array,left,partition-1);
		if(right > partition)
			quicksort(array,partition,right);
	}
	public static int Partition(int[] array, int left, int right) {
		int temp=0;
		int pivot = array[left];
		while(left <= right) {
			while(array[left] < pivot) { left++; }
			while(array[right] > pivot && right>0) { right--; }
			if(left <= right) {
				temp = array[left];
				array[left] = array[right];
				array[right] = temp;
				left++;
				right--;
			}
		}
		return left;
	}
	public static int QSFirst(int[] array,int left,int right) {
		int pivot = array[left];
		int leftptr = left+1, temp=0;
		comparefirst += right-left;
		for(int k=leftptr;k<=right;k++) {
			if(array[k] < pivot) {
				//comparefirst++;
				//System.out.println("comparefirst cnt:"+comparefirst);
				temp = array[k];
				array[k] = array[leftptr];
				array[leftptr] = temp;
				leftptr++;
			}
		}
		temp = array[leftptr-1];
		array[leftptr-1]= array[left];
		array[left] = temp;
		if(left < leftptr-2)
			QSFirst(array,left,leftptr-2);
		if(right > leftptr)
			QSFirst(array,leftptr,right);
		return comparefirst;
	}
	public static int QSFinal(int[] array,int left,int right) {
		int pivot = array[right];
		int leftptr = left, temp=0;
		comparelast += right-left;
		for(int k=leftptr;k<=right-1;k++) {
			if(array[k] < pivot) {
				temp = array[k];
				array[k] = array[leftptr];
				array[leftptr] = temp;
				leftptr++;
			}
		}
		temp = array[leftptr];
		array[leftptr]= array[right];
		array[right] = temp;
		if(left < leftptr-1)
			QSFinal(array,left,leftptr-1);
		if(right > leftptr)
			QSFinal(array,leftptr,right);
		return comparelast;
	}
	public static int QSFinal1(int[] array,int left,int right) {
		int leftptr = left+1, temp=0;
		temp = array[left];
		array[left]= array[right];
		array[right] = temp;
		int pivot = array[left];
		comparelast1 += right-left;
		for(int k=leftptr;k<=right;k++) {
			if(array[k] < pivot) {
				temp = array[k];
				array[k] = array[leftptr];
				array[leftptr] = temp;
				leftptr++;
			}
		}
		temp = array[leftptr-1];
		array[leftptr-1]= array[left];
		array[left] = temp;
		if(left < leftptr-2)
			QSFinal1(array,left,leftptr-2);
		if(right > leftptr)
			QSFinal1(array,leftptr,right);
		return comparelast1;
	}
	public static void main (String[] args) throws IOException {
		int count=0,cnt=0;
		String filename = "C:\\Users\\lavanya\\Desktop\\Matsers\\Design and Algo\\coursera\\QSInput.txt";
		String line = null;
		LineNumberReader lnr = new LineNumberReader(new FileReader(new File(filename)));
		lnr.skip(Long.MAX_VALUE);
		count = lnr.getLineNumber()+1;
		int[] array = new int[count];
		System.out.println("number of inputs:"+count);
		try {
			FileReader filereader = new FileReader(filename);
			BufferedReader bufferedreader = new BufferedReader(filereader);
			while ((line = bufferedreader.readLine()) != null) {
				array[cnt] = Integer.parseInt(line);
				cnt++;
			}
			bufferedreader.close();
		}
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '" 
                    + filename + "'");                  
        }
		int[] input = {69,92,40,76,96,47,30,19,0,31 };
		System.out.print("Before sorting");
		for(int i=0;i<array.length;i++) {
			System.out.print(array[i]+",");
		}
		System.out.println();
		//System.out.println("comparison count is:"+QSFirst(array,0,array.length-1));
		//System.out.println("comparison count is:"+QSFinal(array,0,array.length-1));
		System.out.println("comparison count is:"+QSFinal1(array,0,array.length-1));
		System.out.print("After sorting");
		for(int i=0;i<array.length;i++) {
			System.out.print(array[i]+",");
		}
	}
}
